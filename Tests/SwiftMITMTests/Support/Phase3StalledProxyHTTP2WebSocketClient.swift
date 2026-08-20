import Crypto
import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2

struct Phase3StalledWebSocketResult: Sendable {
    let status: Int
    let payloadBytes: Int
    let digest: [UInt8]
}

final class Phase3StalledWebSocketExchange: @unchecked Sendable {
    let completion: EventLoopFuture<Phase3StalledWebSocketResult>
    let resumedPayload: EventLoopFuture<Int>

    private let connection: Channel
    private let stream: Channel
    private let state: Phase3StalledWebSocketState

    init(
        connection: Channel,
        stream: Channel,
        state: Phase3StalledWebSocketState,
        completion: EventLoopFuture<Phase3StalledWebSocketResult>,
        resumedPayload: EventLoopFuture<Int>
    ) {
        self.connection = connection
        self.stream = stream
        self.state = state
        self.completion = completion
        self.resumedPayload = resumedPayload
    }

    var receivedPayloadBytes: Int { state.receivedPayloadBytes }
    var streamActive: Bool { stream.isActive }
    var connectionActive: Bool { connection.isActive }

    func resume() async throws {
        try await stream.setOption(ChannelOptions.autoRead, value: true).map { self.stream.read() }.get()
        try await connection.setOption(ChannelOptions.autoRead, value: true).map { self.connection.read() }.get()
    }

    func shutdown() async {
        if stream.isActive {
            try? await stream.close().get()
        }
        if connection.isActive {
            try? await connection.close().get()
        }
    }
}

final class Phase3StalledWebSocketState: @unchecked Sendable {
    private let lock = NIOLock()
    private var storedPayloadBytes = 0
    private let progressCompletion: Phase2FixtureCompletion<Int>

    init(eventLoop: EventLoop) {
        progressCompletion = Phase2FixtureCompletion(eventLoop: eventLoop, timeout: .seconds(30))
    }

    var receivedPayloadBytes: Int { lock.withLock { storedPayloadBytes } }
    var resumedPayload: EventLoopFuture<Int> { progressCompletion.futureResult }

    func addPayloadBytes(_ count: Int) {
        let total = lock.withLock { () -> Int in
            storedPayloadBytes += count
            return storedPayloadBytes
        }
        if total > 0 {
            progressCompletion.complete(.success(total))
        }
    }
}

extension Phase3ProxyHTTP2WebSocketClient {
    func beginStalledWebSocketExchange(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        payloadSize: Int = Phase3LargeWebSocketFrame.payloadSize,
        startsStalled: Bool = true
    ) throws -> Phase3StalledWebSocketExchange {
        let channel = try openTunnel(
            proxyPort: proxyPort,
            originHost: originHost,
            originPort: originPort
        )
        do {
            let setup = try configureTLSAndHTTP2(
                channel: channel,
                originHost: originHost,
                mitmCACertificatePEM: mitmCACertificatePEM
            )
            guard try setup.capability.wait() else {
                throw Phase2FixtureError.unexpectedBytes
            }
            return try makeStalledExchange(
                setup: setup,
                channel: channel,
                authority: "\(originHost):\(originPort)",
                payloadSize: payloadSize,
                startsStalled: startsStalled
            )
        } catch {
            try? channel.close().wait()
            throw error
        }
    }

    private func makeStalledExchange(
        setup: Phase3HTTP2ClientSetup,
        channel: Channel,
        authority: String,
        payloadSize: Int,
        startsStalled: Bool
    ) throws -> Phase3StalledWebSocketExchange {
        let state = Phase3StalledWebSocketState(eventLoop: channel.eventLoop)
        let completion = Phase2FixtureCompletion<Phase3StalledWebSocketResult>(
            eventLoop: channel.eventLoop,
            timeout: .seconds(300)
        )
        if startsStalled {
            try channel.setOption(ChannelOptions.autoRead, value: false).wait()
        }
        let stream = try setup.multiplexer.createStreamChannel { stream in
            let readsConfigured = startsStalled
                ? stream.setOption(ChannelOptions.autoRead, value: false)
                : stream.eventLoop.makeSucceededVoidFuture()
            return readsConfigured.flatMapThrowing {
                try stream.pipeline.syncOperations.addHandler(Phase3StalledWebSocketClientHandler(
                    authority: authority,
                    payloadSize: payloadSize,
                    state: state,
                    completion: completion
                ))
            }
        }
        .wait()
        return .init(
            connection: channel,
            stream: stream,
            state: state,
            completion: completion.futureResult,
            resumedPayload: state.resumedPayload
        )
    }
}

private final class Phase3StalledWebSocketClientHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let payloadSize: Int
    private let state: Phase3StalledWebSocketState
    private let completion: Phase2FixtureCompletion<Phase3StalledWebSocketResult>
    private var accepted = false
    private var headerOffset = 0
    private var payloadBytes = 0
    private var hasher = SHA256()
    private var finished = false

    init(
        authority: String,
        payloadSize: Int,
        state: Phase3StalledWebSocketState,
        completion: Phase2FixtureCompletion<Phase3StalledWebSocketResult>
    ) {
        self.authority = authority
        self.payloadSize = payloadSize
        self.state = state
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "CONNECT")
        headers.add(name: ":protocol", value: "websocket")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":path", value: "/load")
        headers.add(name: ":authority", value: authority)
        context.writeAndFlush(wrapOutboundOut(.headers(.init(headers: headers))), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .headers(let frame):
            receive(headers: frame, context: context)
        case .data(let frame):
            receive(data: frame.data, endStream: frame.endStream, context: context)
        default:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !finished {
            completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func receive(
        headers: HTTP2Frame.FramePayload.Headers,
        context: ChannelHandlerContext
    ) {
        guard headers.headers.first(name: ":status") == "200", !headers.endStream else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        accepted = true
    }

    private func receive(data: IOData, endStream: Bool, context: ChannelHandlerContext) {
        guard accepted, case .byteBuffer(let buffer) = data else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        let initialPayloadBytes = payloadBytes
        var readable = buffer
        let header = Phase3LargeWebSocketFrame.header(payloadSize: payloadSize)
        while headerOffset < header.count, let byte = readable.readInteger(as: UInt8.self) {
            guard receiveHeaderByte(byte) else {
                completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
                context.close(promise: nil)
                return
            }
        }
        let receivedPayloadBytes = readable.readableBytes
        readable.withUnsafeReadableBytes { hasher.update(bufferPointer: $0) }
        payloadBytes += receivedPayloadBytes
        state.addPayloadBytes(payloadBytes - initialPayloadBytes)
        guard payloadBytes <= payloadSize else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        guard endStream else { return }
        guard
            headerOffset == header.count,
            payloadBytes == payloadSize
        else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        finished = true
        completion.complete(.success(.init(
            status: 200,
            payloadBytes: payloadBytes,
            digest: Array(hasher.finalize())
        )))
    }

    private func receiveHeaderByte(_ byte: UInt8) -> Bool {
        let expectedHeader = Phase3LargeWebSocketFrame.header(payloadSize: payloadSize)
        guard byte == expectedHeader[headerOffset] else { return false }
        headerOffset += 1
        return true
    }
}

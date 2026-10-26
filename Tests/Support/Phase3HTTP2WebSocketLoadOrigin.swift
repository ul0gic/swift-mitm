import Crypto
import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

enum Phase3LargeWebSocketFrame {
    static let payloadSize = 128 * 1024 * 1024
    static let captureLimit = 64 * 1024
    static let payloadByte: UInt8 = 0xA5
    static let dataChunkSize = 16 * 1024

    static func header(payloadSize: Int) -> [UInt8] {
        let length = UInt64(payloadSize)
        return [0x82, 0x7F] + (0..<8).map { offset in
            UInt8(truncatingIfNeeded: length >> UInt64((7 - offset) * 8))
        }
    }

    static func expectedDigest(payloadSize: Int) -> [UInt8] {
        var hasher = SHA256()
        let chunk = [UInt8](repeating: payloadByte, count: dataChunkSize)
        var remaining = payloadSize
        while remaining > 0 {
            let count = min(remaining, chunk.count)
            chunk.withUnsafeBytes { bytes in
                hasher.update(bufferPointer: UnsafeRawBufferPointer(rebasing: bytes.prefix(count)))
            }
            remaining -= count
        }
        return Array(hasher.finalize())
    }
}

struct Phase3LoadOriginDiagnostic: Sendable {
    let sentBytes: Int
    let remainingBytes: Int
    let streamActive: Bool
    let connectionActive: Bool
    let requestHeadersEndStream: Bool?
}

private final class Phase3LoadOriginState: Sendable {
    private struct State {
        var sentBytes = 0
        var remainingBytes: Int
        var streamActive = false
        var requestHeadersEndStream: Bool?
    }

    private let storage: NIOLockedValueBox<State>

    init(payloadSize: Int) {
        storage = NIOLockedValueBox(State(remainingBytes: payloadSize))
    }

    func setStreamActive(_ active: Bool) {
        storage.withLockedValue { $0.streamActive = active }
    }

    func recordWrite(sentBytes: Int, remainingBytes: Int) {
        storage.withLockedValue {
            $0.sentBytes = sentBytes
            $0.remainingBytes = remainingBytes
        }
    }

    func recordRequestHeaders(endStream: Bool) {
        storage.withLockedValue { $0.requestHeadersEndStream = endStream }
    }

    func diagnostic(connectionActive: Bool) -> Phase3LoadOriginDiagnostic {
        storage.withLockedValue {
            .init(
                sentBytes: $0.sentBytes,
                remainingBytes: $0.remainingBytes,
                streamActive: $0.streamActive,
                connectionActive: connectionActive,
                requestHeadersEndStream: $0.requestHeadersEndStream
            )
        }
    }
}

final class Phase3HTTP2WebSocketLoadOrigin: @unchecked Sendable {
    let caCertificatePEM: String
    let hostname = "localhost"

    private let group: EventLoopGroup
    private let sslContext: NIOSSLContext
    private let payloadSize: Int
    private let state: Phase3LoadOriginState
    private let stalledCompletion: Phase2FixtureCompletion<Int>
    private let sentCompletion: Phase2FixtureCompletion<Int>
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup, payloadSize: Int = Phase3LargeWebSocketFrame.payloadSize) throws {
        self.group = group
        self.payloadSize = payloadSize
        state = Phase3LoadOriginState(payloadSize: payloadSize)
        let ca = try CertificateAuthority(commonName: "SwiftMITM Phase 3 Load Root")
        caCertificatePEM = ca.caCertificatePEM
        let leaf = try ca.leaf(forHost: hostname)
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: leaf.certificateChain,
            privateKey: leaf.privateKey
        )
        configuration.applicationProtocols = ["h2"]
        sslContext = try NIOSSLContext(configuration: configuration)
        stalledCompletion = Phase2FixtureCompletion(eventLoop: group.next(), timeout: .seconds(20))
        sentCompletion = Phase2FixtureCompletion(eventLoop: group.next(), timeout: .seconds(300))
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var stalledBytes: EventLoopFuture<Int> { stalledCompletion.futureResult }
    var sentBytes: EventLoopFuture<Int> { sentCompletion.futureResult }
    var diagnostic: Phase3LoadOriginDiagnostic {
        state.diagnostic(connectionActive: children.withLockedValue { $0.values.contains { $0.isActive } })
    }

    func start() throws {
        let sslContext = sslContext
        let stalledCompletion = stalledCompletion
        let sentCompletion = sentCompletion
        let children = children
        let payloadSize = payloadSize
        let state = state
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                children.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers([
                        NIOSSLServerHandler(context: sslContext),
                        ApplicationProtocolNegotiationHandler { result, channel in
                            Self.configureHTTP2(
                                result: result,
                                channel: channel,
                                payloadSize: payloadSize,
                                state: state,
                                stalledCompletion: stalledCompletion,
                                sentCompletion: sentCompletion
                            )
                        }
                    ])
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
        try? channel?.close().wait()
        stalledCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        sentCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }

    private static func configureHTTP2(
        result: ALPNResult,
        channel: Channel,
        payloadSize: Int,
        state: Phase3LoadOriginState,
        stalledCompletion: Phase2FixtureCompletion<Int>,
        sentCompletion: Phase2FixtureCompletion<Int>
    ) -> EventLoopFuture<Void> {
        guard case .negotiated("h2") = result else {
            return channel.eventLoop.makeFailedFuture(Phase2FixtureError.unexpectedBytes)
        }
        var mutableConfiguration = NIOHTTP2Handler.ConnectionConfiguration()
        mutableConfiguration.initialSettings = [HTTP2Setting(parameter: .enableConnectProtocol, value: 1)]
        let configuration = mutableConfiguration
        return channel.configureHTTP2Pipeline(
            mode: .server,
            connectionConfiguration: configuration,
            streamConfiguration: .init()
        ) { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase3LoadOriginStreamHandler(
                    payloadSize: payloadSize,
                    state: state,
                    stalledCompletion: stalledCompletion,
                    sentCompletion: sentCompletion
                ))
            }
        }
        .map { _ in () }
    }
}

private final class Phase3LoadOriginStreamHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let stalledCompletion: Phase2FixtureCompletion<Int>
    private let sentCompletion: Phase2FixtureCompletion<Int>
    private let payloadSize: Int
    private let state: Phase3LoadOriginState
    private var remaining: Int
    private var sent = 0
    private var writeInFlight = false
    private var started = false

    init(
        payloadSize: Int,
        state: Phase3LoadOriginState,
        stalledCompletion: Phase2FixtureCompletion<Int>,
        sentCompletion: Phase2FixtureCompletion<Int>
    ) {
        self.payloadSize = payloadSize
        self.state = state
        remaining = payloadSize
        self.stalledCompletion = stalledCompletion
        self.sentCompletion = sentCompletion
    }

    func handlerAdded(context: ChannelHandlerContext) {
        state.setStreamActive(true)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case .headers(let frame) = unwrapInboundIn(data), !started else { return }
        state.recordRequestHeaders(endStream: frame.endStream)
        let headers = frame.headers
        guard
            headers.first(name: ":method") == "CONNECT",
            headers.first(name: ":protocol") == "websocket",
            headers.first(name: ":path") == "/load"
        else {
            context.close(promise: nil)
            return
        }
        started = true
        sendResponse(context: context)
        pump(context: context)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        if context.channel.isWritable {
            pump(context: context)
        } else if started, remaining > 0 {
            stalledCompletion.complete(.success(sent))
        }
        context.fireChannelWritabilityChanged()
    }

    func channelInactive(context: ChannelHandlerContext) {
        state.setStreamActive(false)
        if remaining > 0 {
            sentCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        sentCompletion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func sendResponse(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: "200")
        context.write(wrapOutboundOut(.headers(.init(headers: headers))), promise: nil)
        let header = ByteBuffer(bytes: Phase3LargeWebSocketFrame.header(payloadSize: payloadSize))
        context.write(wrapOutboundOut(.data(.init(data: .byteBuffer(header), endStream: false))), promise: nil)
        context.flush()
    }

    private func pump(context: ChannelHandlerContext) {
        guard started, !writeInFlight, remaining > 0 else { return }
        guard context.channel.isWritable else {
            stalledCompletion.complete(.success(sent))
            return
        }
        let count = min(Phase3LargeWebSocketFrame.dataChunkSize, remaining)
        var buffer = context.channel.allocator.buffer(capacity: count)
        buffer.writeRepeatingByte(Phase3LargeWebSocketFrame.payloadByte, count: count)
        remaining -= count
        sent += count
        state.recordWrite(sentBytes: sent, remainingBytes: remaining)
        writeInFlight = true
        let writePromise = context.eventLoop.makePromise(of: Void.self)
        let loopBoundHandler = NIOLoopBound(self, eventLoop: context.eventLoop)
        let loopBoundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
        writePromise.futureResult.whenComplete { result in
            let handler = loopBoundHandler.value
            let context = loopBoundContext.value
            handler.writeInFlight = false
            switch result {
            case .success:
                if handler.remaining == 0 {
                    handler.sentCompletion.complete(.success(handler.sent))
                } else {
                    handler.pump(context: context)
                }
            case .failure(let error):
                handler.sentCompletion.complete(.failure(error))
                context.close(promise: nil)
            }
        }
        let endStream = remaining == 0
        let frame = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(buffer), endStream: endStream))
        context.writeAndFlush(wrapOutboundOut(frame), promise: writePromise)
    }
}

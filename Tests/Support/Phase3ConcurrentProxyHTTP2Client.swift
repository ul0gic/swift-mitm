import NIOCore
import NIOHPACK
import NIOHTTP2

struct Phase3ConcurrentClientResult: Sendable {
    let ordinaryResponseBytes: [UInt8]
    let socketAResponseBytes: [UInt8]
    let socketBFailedAfterAcceptance: Bool
}

extension Phase3ProxyHTTP2WebSocketClient {
    func concurrentExchange(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String
    ) throws -> Phase3ConcurrentClientResult {
        let channel = try openTunnel(
            proxyPort: proxyPort,
            originHost: originHost,
            originPort: originPort
        )
        defer { try? channel.close().wait() }
        let setup = try configureTLSAndHTTP2(
            channel: channel,
            originHost: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM
        )
        guard try setup.capability.wait() else {
            throw Phase2FixtureError.unexpectedBytes
        }
        let ordinaryCompletion = Phase2FixtureCompletion<[UInt8]>(eventLoop: channel.eventLoop)
        let socketACompletion = Phase2FixtureCompletion<[UInt8]>(eventLoop: channel.eventLoop)
        let socketBCompletion = Phase2FixtureCompletion<Bool>(eventLoop: channel.eventLoop)
        let authority = "\(originHost):\(originPort)"

        let ordinary = try makeOrdinaryStream(
            multiplexer: setup.multiplexer,
            authority: authority,
            completion: ordinaryCompletion
        )
        let socketA = try makeWebSocketStream(
            multiplexer: setup.multiplexer,
            authority: authority,
            path: "/socket/a",
            completion: socketACompletion
        )
        let socketB = try makeFailingWebSocketStream(
            multiplexer: setup.multiplexer,
            authority: authority,
            completion: socketBCompletion
        )
        defer {
            try? ordinary.close().wait()
            try? socketA.close().wait()
            try? socketB.close().wait()
        }

        return try .init(
            ordinaryResponseBytes: ordinaryCompletion.futureResult.wait(),
            socketAResponseBytes: socketACompletion.futureResult.wait(),
            socketBFailedAfterAcceptance: socketBCompletion.futureResult.wait()
        )
    }

    private func makeOrdinaryStream(
        multiplexer: NIOHTTP2Handler.StreamMultiplexer,
        authority: String,
        completion: Phase2FixtureCompletion<[UInt8]>
    ) throws -> Channel {
        try multiplexer.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase3OrdinaryHTTP2ClientHandler(
                    authority: authority,
                    completion: completion
                ))
            }
        }
        .wait()
    }

    private func makeWebSocketStream(
        multiplexer: NIOHTTP2Handler.StreamMultiplexer,
        authority: String,
        path: String,
        completion: Phase2FixtureCompletion<[UInt8]>
    ) throws -> Channel {
        try multiplexer.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase3IsolatedWebSocketClientHandler(
                    authority: authority,
                    path: path,
                    completion: completion
                ))
            }
        }
        .wait()
    }

    private func makeFailingWebSocketStream(
        multiplexer: NIOHTTP2Handler.StreamMultiplexer,
        authority: String,
        completion: Phase2FixtureCompletion<Bool>
    ) throws -> Channel {
        try multiplexer.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase3FailingWebSocketClientHandler(
                    authority: authority,
                    completion: completion
                ))
            }
        }
        .wait()
    }
}

private final class Phase3OrdinaryHTTP2ClientHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let completion: Phase2FixtureCompletion<[UInt8]>
    private var accepted = false
    private var bytes: [UInt8] = []

    init(authority: String, completion: Phase2FixtureCompletion<[UInt8]>) {
        self.authority = authority
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "GET")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":path", value: "/ordinary")
        headers.add(name: ":authority", value: authority)
        context.writeAndFlush(wrapOutboundOut(.headers(.init(headers: headers, endStream: true))), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .headers(let frame):
            accepted = frame.headers.first(name: ":status") == "200"
        case .data(let frame):
            receive(data: frame.data, endStream: frame.endStream, context: context)
        default:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func receive(data: IOData, endStream: Bool, context: ChannelHandlerContext) {
        guard accepted, case .byteBuffer(let buffer) = data else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        bytes.append(contentsOf: buffer.readableBytesView)
        if endStream {
            completion.complete(.success(bytes))
        }
    }
}

private final class Phase3IsolatedWebSocketClientHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let path: String
    private let completion: Phase2FixtureCompletion<[UInt8]>
    private var accepted = false
    private var bytes: [UInt8] = []
    private var sentClose = false

    init(authority: String, path: String, completion: Phase2FixtureCompletion<[UInt8]>) {
        self.authority = authority
        self.path = path
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        sendHeaders(authority: authority, path: path, context: context)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .headers(let frame):
            accepted = frame.headers.first(name: ":status") == "200"
            if accepted {
                send(WebSocketWire.clientTextFrame, context: context)
            }
        case .data(let frame):
            receive(data: frame.data, context: context)
        default:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func receive(data: IOData, context: ChannelHandlerContext) {
        guard accepted, case .byteBuffer(let buffer) = data else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        bytes.append(contentsOf: buffer.readableBytesView)
        let expected = WebSocketWire.serverFrames
        guard bytes.count <= expected.count, bytes == Array(expected.prefix(bytes.count)) else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        if bytes.count >= WebSocketWire.serverBinaryFrame.count, !sentClose {
            sentClose = true
            send(WebSocketWire.clientCloseFrame, context: context)
        }
        if bytes.count == expected.count {
            completion.complete(.success(bytes))
        }
    }

    private func send(_ bytes: [UInt8], context: ChannelHandlerContext) {
        let buffer = ByteBuffer(bytes: bytes)
        context.writeAndFlush(
            wrapOutboundOut(.data(.init(data: .byteBuffer(buffer), endStream: false))),
            promise: nil
        )
    }
}

private final class Phase3FailingWebSocketClientHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let completion: Phase2FixtureCompletion<Bool>
    private var accepted = false
    private var sentText = false

    init(authority: String, completion: Phase2FixtureCompletion<Bool>) {
        self.authority = authority
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        sendHeaders(authority: authority, path: "/socket/b", context: context)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case .headers(let frame) = unwrapInboundIn(data) else { return }
        accepted = frame.headers.first(name: ":status") == "200"
        if accepted {
            sentText = true
            let buffer = ByteBuffer(bytes: WebSocketWire.clientTextFrame)
            let data = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(buffer), endStream: false))
            context.writeAndFlush(wrapOutboundOut(data), promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        completeForFailure()
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completeForFailure()
        context.close(promise: nil)
    }

    private func completeForFailure() {
        let result: Result<Bool, Error> = accepted && sentText
            ? .success(true)
            : .failure(Phase2FixtureError.closedBeforeExpectedBytes)
        completion.complete(result)
    }
}

private func sendHeaders(authority: String, path: String, context: ChannelHandlerContext) {
    var headers = HPACKHeaders()
    headers.add(name: ":method", value: "CONNECT")
    headers.add(name: ":protocol", value: "websocket")
    headers.add(name: ":scheme", value: "https")
    headers.add(name: ":path", value: path)
    headers.add(name: ":authority", value: authority)
    context.writeAndFlush(NIOAny(HTTP2Frame.FramePayload.headers(.init(headers: headers))), promise: nil)
}

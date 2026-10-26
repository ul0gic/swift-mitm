import NIOCore
import NIOConcurrencyHelpers
import NIOHPACK
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

struct Phase3ProxyHTTP2WebSocketClientResult: Sendable {
    let downstreamExtendedConnectEnabled: Bool
    let status: Int?
    let serverBytes: [UInt8]
}

final class Phase3ProxyHTTP2WebSocketClient: Sendable {
    private struct State {
        var channel: Channel?
        var stream: Channel?
    }

    private let group: EventLoopGroup
    private let state = NIOLockedValueBox(State())

    init(group: EventLoopGroup) {
        self.group = group
    }

    func exchange(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        sendExtendedConnect: Bool = true
    ) throws -> Phase3ProxyHTTP2WebSocketClientResult {
        let channel = try openTunnel(
            proxyPort: proxyPort,
            originHost: originHost,
            originPort: originPort
        )
        state.withLockedValue { $0.channel = channel }
        let setup = try configureTLSAndHTTP2(
            channel: channel,
            originHost: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM
        )
        return try exchange(
            setup: setup,
            channel: channel,
            originHost: originHost,
            originPort: originPort,
            sendExtendedConnect: sendExtendedConnect
        )
    }

    func exchangeDirectly(
        originHost: String,
        originPort: Int,
        originCACertificatePEM: String
    ) throws -> Phase3ProxyHTTP2WebSocketClientResult {
        let channel = try ClientBootstrap(group: group)
            .connect(host: "127.0.0.1", port: originPort)
            .wait()
        state.withLockedValue { $0.channel = channel }
        let setup = try configureTLSAndHTTP2(
            channel: channel,
            originHost: originHost,
            mitmCACertificatePEM: originCACertificatePEM
        )
        return try exchange(
            setup: setup,
            channel: channel,
            originHost: originHost,
            originPort: originPort,
            sendExtendedConnect: true
        )
    }

    private func exchange(
        setup: Phase3HTTP2ClientSetup,
        channel: Channel,
        originHost: String,
        originPort: Int,
        sendExtendedConnect: Bool
    ) throws -> Phase3ProxyHTTP2WebSocketClientResult {
        let capability = try setup.capability.wait()
        guard sendExtendedConnect, capability else {
            return .init(
                downstreamExtendedConnectEnabled: capability,
                status: nil,
                serverBytes: []
            )
        }

        let completion = Phase2FixtureCompletion<Phase3ProxyHTTP2WebSocketClientResult>(
            eventLoop: channel.eventLoop
        )
        let stream = try setup.multiplexer.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase3ProxyHTTP2WebSocketClientStreamHandler(
                    capability: capability,
                    authority: "\(originHost):\(originPort)",
                    completion: completion
                ))
            }
        }
        .wait()
        state.withLockedValue { $0.stream = stream }
        return try completion.futureResult.wait()
    }

    func stop() {
        let channels = state.withLockedValue { ($0.stream, $0.channel) }
        try? channels.0?.close().wait()
        try? channels.1?.close().wait()
    }

    func openTunnel(proxyPort: Int, originHost: String, originPort: Int) throws -> Channel {
        let completion = Phase2FixtureCompletion<Void>(eventLoop: group.next())
        let channel = try ClientBootstrap(group: group)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        Phase3ConnectResponseHandler(completion: completion),
                        name: "phase3-connect-response"
                    )
                }
            }
            .connect(host: "127.0.0.1", port: proxyPort)
            .wait()
        var request = ByteBuffer()
        request.writeString(
            "CONNECT \(originHost):\(originPort) HTTP/1.1\r\nHost: \(originHost):\(originPort)\r\n\r\n"
        )
        try channel.writeAndFlush(request).wait()
        try completion.futureResult.wait()
        try channel.pipeline.removeHandler(name: "phase3-connect-response").wait()
        return channel
    }

    func configureTLSAndHTTP2(
        channel: Channel,
        originHost: String,
        mitmCACertificatePEM: String,
        capabilityTimeout: TimeAmount = .seconds(2)
    ) throws -> Phase3HTTP2ClientSetup {
        var configuration = TLSConfiguration.makeClientConfiguration()
        configuration.applicationProtocols = ["h2"]
        configuration.certificateVerification = .fullVerification
        let trustRoot = try NIOSSLCertificate(
            bytes: Array(mitmCACertificatePEM.utf8),
            format: .pem
        )
        configuration.trustRoots = .certificates([trustRoot])
        let sslContext = try NIOSSLContext(configuration: configuration)
        let handshake = Phase2FixtureCompletion<String?>(eventLoop: channel.eventLoop)
        let capability = Phase2FixtureCompletion<Bool>(
            eventLoop: channel.eventLoop,
            timeout: capabilityTimeout
        )
        let multiplexer = Phase2FixtureCompletion<NIOHTTP2Handler.StreamMultiplexer>(
            eventLoop: channel.eventLoop
        )

        let configured = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandlers([
                NIOSSLClientHandler(context: sslContext, serverHostname: originHost),
                Phase3TLSHandshakeHandler(completion: handshake),
                Phase3HTTP2SettingsObserver(completion: capability)
            ])
        }
        .flatMap {
            channel.configureHTTP2SecureUpgrade { channel in
                channel.configureHTTP2Pipeline(
                    mode: .client,
                    connectionConfiguration: .init(),
                    streamConfiguration: .init()
                ) { $0.close() }
                .map { value in multiplexer.complete(.success(value)) }
            } http1ChannelConfigurator: { channel in
                let error = Phase2FixtureError.unexpectedBytes
                multiplexer.complete(.failure(error))
                return channel.eventLoop.makeFailedFuture(error)
            }
        }
        try configured.wait()
        guard try handshake.futureResult.wait() == "h2" else {
            throw Phase2FixtureError.unexpectedBytes
        }
        return try .init(
            multiplexer: multiplexer.futureResult.wait(),
            capability: capability.futureResult
        )
    }
}

struct Phase3HTTP2ClientSetup {
    let multiplexer: NIOHTTP2Handler.StreamMultiplexer
    let capability: EventLoopFuture<Bool>
}

private final class Phase3ConnectResponseHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private static let maximumResponseBytes = 4_096

    private let completion: Phase2FixtureCompletion<Void>
    private var bytes: [UInt8] = []

    init(completion: Phase2FixtureCompletion<Void>) {
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        bytes.append(contentsOf: buffer.readableBytesView)
        guard bytes.count <= Self.maximumResponseBytes else {
            completion.complete(.failure(Phase2FixtureError.exceededByteLimit))
            context.close(promise: nil)
            return
        }
        guard bytes.firstRange(of: [13, 10, 13, 10]) != nil else { return }
        let prefix = Array("HTTP/1.1 200".utf8)
        let result: Result<Void, Error> = bytes.starts(with: prefix)
            ? .success(())
            : .failure(Phase2FixtureError.unexpectedBytes)
        completion.complete(result)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

private final class Phase3TLSHandshakeHandler: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let completion: Phase2FixtureCompletion<String?>

    init(completion: Phase2FixtureCompletion<String?>) {
        self.completion = completion
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let protocolName) = event {
            completion.complete(.success(protocolName))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

private final class Phase3ProxyHTTP2WebSocketClientStreamHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let capability: Bool
    private let authority: String
    private let completion: Phase2FixtureCompletion<Phase3ProxyHTTP2WebSocketClientResult>
    private var status: Int?
    private var serverBytes: [UInt8] = []
    private var sentText = false
    private var sentClose = false

    init(
        capability: Bool,
        authority: String,
        completion: Phase2FixtureCompletion<Phase3ProxyHTTP2WebSocketClientResult>
    ) {
        self.capability = capability
        self.authority = authority
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "CONNECT")
        headers.add(name: ":protocol", value: "websocket")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":path", value: "/socket")
        headers.add(name: ":authority", value: authority)
        context.writeAndFlush(wrapOutboundOut(.headers(.init(headers: headers))), promise: nil)
        context.fireChannelActive()
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
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
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
        guard let statusValue = headers.headers.first(name: ":status"), let status = Int(statusValue) else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        self.status = status
        if headers.endStream || !(200 ... 299).contains(status) {
            completion.complete(.success(.init(
                downstreamExtendedConnectEnabled: capability,
                status: status,
                serverBytes: serverBytes
            )))
            return
        }
        sendText(context: context)
    }

    private func receive(data: IOData, endStream: Bool, context: ChannelHandlerContext) {
        guard status.map({ (200 ... 299).contains($0) }) == true, case .byteBuffer(let buffer) = data else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        serverBytes.append(contentsOf: buffer.readableBytesView)
        let expected = WebSocketWire.serverFrames
        guard
            serverBytes.count <= expected.count,
            serverBytes == Array(expected.prefix(serverBytes.count))
        else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        if serverBytes.count >= WebSocketWire.serverBinaryFrame.count, !sentClose {
            sentClose = true
            send(WebSocketWire.clientCloseFrame, context: context)
        }
        if serverBytes.count == expected.count || endStream {
            completion.complete(.success(.init(
                downstreamExtendedConnectEnabled: capability,
                status: status,
                serverBytes: serverBytes
            )))
        }
    }

    private func sendText(context: ChannelHandlerContext) {
        guard !sentText else { return }
        sentText = true
        send(WebSocketWire.clientTextFrame, context: context)
    }

    private func send(_ bytes: [UInt8], context: ChannelHandlerContext) {
        let buffer = ByteBuffer(bytes: bytes)
        let frame = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(buffer), endStream: false))
        context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
    }
}

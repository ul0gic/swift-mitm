import NIOCore
import NIOHPACK
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

final class ProxyTestClient {
    private struct TLSInstallation {
        let handshake: EventLoopFuture<String?>
        let multiplexer: EventLoopFuture<NIOHTTP2Handler.StreamMultiplexer?>?
    }

    let group: EventLoopGroup
    init(group: EventLoopGroup) {
        self.group = group
    }
    func holdConnection(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        alpn: String
    ) throws -> Channel {
        let channel = try openTunnel(proxyPort: proxyPort, originHost: originHost, originPort: originPort)
        _ = try startTLS(
            on: channel,
            serverHostname: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM,
            alpn: alpn
        )
        return channel
    }

    func fetch(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        alpn: String
    ) throws -> Int {
        try fetch(
            proxyPort: proxyPort,
            originHost: originHost,
            originPort: originPort,
            mitmCACertificatePEM: mitmCACertificatePEM,
            applicationProtocols: [alpn],
            expectedALPN: alpn
        )
    }

    func fetch(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        applicationProtocols: [String],
        expectedALPN: String?,
        timeout: TimeAmount = .seconds(20)
    ) throws -> Int {
        let completion = ProxyFetchCompletion(eventLoop: group.next())
        let timeoutTask = completion.futureResult.eventLoop.scheduleTask(in: timeout) {
            completion.complete(.failure(completion.timeoutError()))
        }
        completion.futureResult.whenComplete { _ in timeoutTask.cancel() }

        do {
            let channel = try openTunnel(proxyPort: proxyPort, originHost: originHost, originPort: originPort)
            defer { try? channel.close().wait() }
            completion.advance(to: .tlsHandshake)
            let multiplexer = try startTLS(
                on: channel,
                serverHostname: originHost,
                mitmCACertificatePEM: mitmCACertificatePEM,
                applicationProtocols: applicationProtocols,
                expectedALPN: expectedALPN
            )

            completion.advance(to: .request)
            let authority = "\(originHost):\(originPort)"
            if expectedALPN == "h2" {
                guard let multiplexer else { throw ProxyTestError.unexpectedALPN }
                try sendHTTP2Request(
                    multiplexer: multiplexer,
                    authority: authority,
                    completion: completion
                )
            } else {
                let installFuture = channel.eventLoop.submit {
                    try channel.pipeline.syncOperations.addHandlers([
                        HTTPRequestEncoder(),
                        ByteToMessageHandler(HTTPResponseDecoder()),
                        H1ConsumingClient(authority: authority, completion: completion)
                    ])
                }
                try installFuture.wait()
            }

            return try completion.futureResult.wait()
        } catch {
            completion.complete(.failure(error))
            throw error
        }
    }

    func beginTLSHandshake(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        applicationProtocols: [String]
    ) throws -> Channel {
        let channel = try openTunnel(proxyPort: proxyPort, originHost: originHost, originPort: originPort)
        _ = try installTLS(
            on: channel,
            serverHostname: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM,
            applicationProtocols: applicationProtocols
        )
        return channel
    }

    func openTunnel(proxyPort: Int, originHost: String, originPort: Int) throws -> Channel {
        let connectDone = group.next().makePromise(of: Void.self)
        let channel = try ClientBootstrap(group: group)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        ConnectResponseHandler(promise: connectDone),
                        name: "connect-response"
                    )
                }
            }
            .connect(host: "127.0.0.1", port: proxyPort)
            .wait()

        var request = channel.allocator.buffer(capacity: 64)
        request.writeString("CONNECT \(originHost):\(originPort) HTTP/1.1\r\nHost: \(originHost):\(originPort)\r\n\r\n")
        try channel.writeAndFlush(request).wait()
        try connectDone.futureResult.wait()
        try channel.pipeline.removeHandler(name: "connect-response").wait()
        return channel
    }

    func startTLS(
        on channel: Channel,
        serverHostname: String,
        mitmCACertificatePEM: String,
        alpn: String
    ) throws -> NIOHTTP2Handler.StreamMultiplexer? {
        try startTLS(
            on: channel,
            serverHostname: serverHostname,
            mitmCACertificatePEM: mitmCACertificatePEM,
            applicationProtocols: [alpn],
            expectedALPN: alpn
        )
    }

    private func startTLS(
        on channel: Channel,
        serverHostname: String,
        mitmCACertificatePEM: String,
        applicationProtocols: [String],
        expectedALPN: String?
    ) throws -> NIOHTTP2Handler.StreamMultiplexer? {
        let installation = try installTLS(
            on: channel,
            serverHostname: serverHostname,
            mitmCACertificatePEM: mitmCACertificatePEM,
            applicationProtocols: applicationProtocols,
            configuresHTTP2: applicationProtocols.contains("h2")
        )
        guard try installation.handshake.wait() == expectedALPN else {
            throw ProxyTestError.unexpectedALPN
        }
        return try installation.multiplexer?.wait()
    }

    private func installTLS(
        on channel: Channel,
        serverHostname: String,
        mitmCACertificatePEM: String,
        applicationProtocols: [String],
        configuresHTTP2: Bool = false
    ) throws -> TLSInstallation {
        var tls = TLSConfiguration.makeClientConfiguration()
        tls.applicationProtocols = applicationProtocols
        tls.certificateVerification = .fullVerification
        let trustRoot = try NIOSSLCertificate(bytes: Array(mitmCACertificatePEM.utf8), format: .pem)
        tls.trustRoots = .certificates([trustRoot])
        let sslContext = try NIOSSLContext(configuration: tls)
        let handshake = channel.eventLoop.makePromise(of: String?.self)
        let multiplexer = configuresHTTP2
            ? channel.eventLoop.makePromise(of: NIOHTTP2Handler.StreamMultiplexer?.self)
            : nil
        let installFuture = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandler(
                NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname),
                position: .first
            )
            try channel.pipeline.syncOperations.addHandler(TLSHandshakeProbe(promise: handshake))
        }
        .flatMap {
            guard let multiplexer else {
                return channel.eventLoop.makeSucceededVoidFuture()
            }
            return channel.configureHTTP2SecureUpgrade { channel in
                let configured = channel.configureHTTP2Pipeline(
                    mode: .client,
                    connectionConfiguration: .init(),
                    streamConfiguration: .init()
                ) { $0.close() }
                configured.map(Optional.some).cascade(to: multiplexer)
                return configured.map { _ in () }
            } http1ChannelConfigurator: { channel in
                multiplexer.succeed(nil)
                return channel.eventLoop.makeSucceededVoidFuture()
            }
        }
        try installFuture.wait()
        return TLSInstallation(
            handshake: handshake.futureResult,
            multiplexer: multiplexer?.futureResult
        )
    }

    private func sendHTTP2Request(
        multiplexer: NIOHTTP2Handler.StreamMultiplexer,
        authority: String,
        completion: ProxyFetchCompletion
    ) throws {
        let streamFuture = multiplexer.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(
                    H2ConsumingClient(authority: authority, completion: completion)
                )
            }
        }
        _ = try streamFuture.wait()
    }
}

private final class H2ConsumingClient: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let completion: ProxyFetchCompletion
    private var bytes = 0
    private var finished = false

    init(authority: String, completion: ProxyFetchCompletion) {
        self.authority = authority
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "GET")
        headers.add(name: ":path", value: "/stream")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":authority", value: authority)
        context.writeAndFlush(wrapOutboundOut(.headers(.init(headers: headers, endStream: true))), promise: nil)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .data(let frame):
            let count = frame.data.readableBytes
            bytes += count
            completion.addReceivedBytes(count)
            if frame.endStream {
                complete()
            }
        case .headers(let frame):
            completion.advance(to: .response)
            if frame.endStream {
                complete()
            }
        default:
            break
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !finished {
            completion.complete(.failure(ProxyTestError.connectionClosedBeforeResponseEnd))
        }
        context.fireChannelInactive()
    }

    private func complete() {
        guard !finished else { return }
        finished = true
        completion.complete(.success(bytes))
    }
}

private final class H1ConsumingClient: ChannelInboundHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart

    private let authority: String
    private let completion: ProxyFetchCompletion
    private var bytes = 0
    private var finished = false
    private var sent = false

    init(authority: String, completion: ProxyFetchCompletion) {
        self.authority = authority
        self.completion = completion
    }

    func handlerAdded(context: ChannelHandlerContext) {
        if context.channel.isActive {
            sendRequest(context: context)
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        sendRequest(context: context)
        context.fireChannelActive()
    }

    private func sendRequest(context: ChannelHandlerContext) {
        guard !sent else { return }
        sent = true
        var headers = HTTPHeaders()
        headers.add(name: "host", value: authority)
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: "/", headers: headers)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head:
            completion.advance(to: .response)
        case .body(let buffer):
            bytes += buffer.readableBytes
            completion.addReceivedBytes(buffer.readableBytes)
        case .end:
            complete()
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !finished {
            completion.complete(.failure(ProxyTestError.connectionClosedBeforeResponseEnd))
        }
        context.fireChannelInactive()
    }

    private func complete() {
        guard !finished else { return }
        finished = true
        completion.complete(.success(bytes))
    }
}

import NIOCore
import NIOHPACK
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

/// A TLS origin that ALPN-branches: h2 streams a fixed body, http/1.1 returns a Content-Length body.
final class TLSOriginServer {
    let ca: CertificateAuthority
    let hostname = "localhost"

    private let group: EventLoopGroup
    private let bodySize: Int
    private var channel: Channel?

    init(group: EventLoopGroup, bodySize: Int) throws {
        self.group = group
        self.bodySize = bodySize
        self.ca = try CertificateAuthority(commonName: "APIGhost Origin Root")
    }

    var caCertificatePEM: String { ca.caCertificatePEM }
    var localPort: Int { channel?.localAddress?.port ?? 0 }

    func start() throws {
        let leaf = try ca.leaf(forHost: hostname)
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: leaf.certificateChain,
            privateKey: leaf.privateKey
        )
        configuration.applicationProtocols = ["h2", "http/1.1"]
        let sslContext = try NIOSSLContext(configuration: configuration)
        let bodySize = bodySize

        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(NIOSSLServerHandler(context: sslContext))
                    let alpn = ApplicationProtocolNegotiationHandler { result, channel in
                        Self.configureProtocol(result: result, channel: channel, bodySize: bodySize)
                    }
                    try channel.pipeline.syncOperations.addHandler(alpn)
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        try? channel?.close().wait()
    }

    private static func configureProtocol(
        result: ALPNResult,
        channel: Channel,
        bodySize: Int
    ) -> EventLoopFuture<Void> {
        if case .negotiated("h2") = result {
            return channel.configureHTTP2Pipeline(
                mode: .server,
                connectionConfiguration: .init(),
                streamConfiguration: .init()
            ) { stream in
                stream.eventLoop.makeCompletedFuture {
                    try stream.pipeline.syncOperations.addHandler(
                        StreamingResponder(bodySize: bodySize, chunkSize: 16 * 1024)
                    )
                }
            }
            .map { _ in () }
        }
        return channel.pipeline.configureHTTPServerPipeline().flatMap {
            channel.pipeline.addHandler(OriginHTTP1Responder(bodySize: bodySize))
        }
    }
}

private final class OriginHTTP1Responder: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let bodySize: Int

    init(bodySize: Int) {
        self.bodySize = bodySize
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case .end = unwrapInboundIn(data) else { return }
        var headers = HTTPHeaders()
        headers.add(name: "content-length", value: String(bodySize))
        headers.add(name: "content-type", value: "application/octet-stream")
        let head = HTTPResponseHead(version: .http1_1, status: .ok, headers: headers)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        var buffer = context.channel.allocator.buffer(capacity: bodySize)
        buffer.writeRepeatingByte(0x41, count: bodySize)
        context.write(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }
}

/// Connects to the proxy, performs CONNECT, completes TLS trusting the MITM CA, then fetches a body.
final class ProxyTestClient {
    private let group: EventLoopGroup

    init(group: EventLoopGroup) {
        self.group = group
    }

    func fetch(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        alpn: String
    ) throws -> Int {
        let done = group.next().makePromise(of: Int.self)
        group.next().scheduleTask(in: .seconds(20)) { done.fail(ProxyTestError.timeout) }
        let channel = try openTunnel(proxyPort: proxyPort, originHost: originHost, originPort: originPort)
        try startTLS(on: channel, serverHostname: originHost, mitmCACertificatePEM: mitmCACertificatePEM, alpn: alpn)

        let authority = "\(originHost):\(originPort)"
        if alpn == "h2" {
            try sendHTTP2Request(on: channel, authority: authority, done: done)
        } else {
            let handlers = channel.pipeline.addHandlers([
                HTTPRequestEncoder(),
                ByteToMessageHandler(HTTPResponseDecoder()),
                H1ConsumingClient(authority: authority, done: done)
            ])
            try handlers.wait()
        }

        let received = try done.futureResult.wait()
        try? channel.close().wait()
        return received
    }

    private func openTunnel(proxyPort: Int, originHost: String, originPort: Int) throws -> Channel {
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

    private func startTLS(
        on channel: Channel,
        serverHostname: String,
        mitmCACertificatePEM: String,
        alpn: String
    ) throws {
        var tls = TLSConfiguration.makeClientConfiguration()
        tls.applicationProtocols = [alpn]
        tls.certificateVerification = .fullVerification
        let trustRoot = try NIOSSLCertificate(bytes: Array(mitmCACertificatePEM.utf8), format: .pem)
        tls.trustRoots = .certificates([trustRoot])
        let sslContext = try NIOSSLContext(configuration: tls)
        let added = channel.pipeline.addHandler(
            try NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname),
            position: .first
        )
        try added.wait()
    }

    private func sendHTTP2Request(on channel: Channel, authority: String, done: EventLoopPromise<Int>) throws {
        let muxFuture = channel.configureHTTP2Pipeline(
            mode: .client,
            connectionConfiguration: .init(),
            streamConfiguration: .init()
        ) { $0.close() }
        let mux = try muxFuture.wait()
        let streamFuture = mux.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(H2ConsumingClient(authority: authority, done: done))
            }
        }
        _ = try streamFuture.wait()
    }
}

private final class ConnectResponseHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let promise: EventLoopPromise<Void>
    private var accumulated = ByteBuffer()

    init(promise: EventLoopPromise<Void>) {
        self.promise = promise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        accumulated.writeBuffer(&buffer)
        guard accumulated.readableBytesView.firstRange(of: [13, 10, 13, 10]) != nil else { return }
        let head = accumulated.getString(at: accumulated.readerIndex, length: accumulated.readableBytes) ?? ""
        if head.contains(" 200 ") {
            promise.succeed(())
        } else {
            promise.fail(ProxyTestError.connectFailed(head))
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        promise.fail(error)
        context.close(promise: nil)
    }
}

private final class H2ConsumingClient: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let done: EventLoopPromise<Int>
    private var bytes = 0
    private var finished = false

    init(authority: String, done: EventLoopPromise<Int>) {
        self.authority = authority
        self.done = done
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
            bytes += frame.data.readableBytes
            if frame.endStream { complete() }
        case .headers(let frame):
            if frame.endStream { complete() }
        default:
            break
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        done.fail(error)
        context.close(promise: nil)
    }

    private func complete() {
        guard !finished else { return }
        finished = true
        done.succeed(bytes)
    }
}

private final class H1ConsumingClient: ChannelInboundHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart

    private let authority: String
    private let done: EventLoopPromise<Int>
    private var bytes = 0
    private var finished = false
    private var sent = false

    init(authority: String, done: EventLoopPromise<Int>) {
        self.authority = authority
        self.done = done
    }

    func handlerAdded(context: ChannelHandlerContext) {
        if context.channel.isActive { sendRequest(context: context) }
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
            break
        case .body(let buffer):
            bytes += buffer.readableBytes
        case .end:
            complete()
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        done.fail(error)
        context.close(promise: nil)
    }

    private func complete() {
        guard !finished else { return }
        finished = true
        done.succeed(bytes)
    }
}

enum ProxyTestError: Error {
    case connectFailed(String)
    case timeout
}

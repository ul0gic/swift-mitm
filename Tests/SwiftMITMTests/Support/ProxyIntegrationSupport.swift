import NIOCore
import NIOHPACK
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

final class ProxyTestClient {
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
        try startTLS(on: channel, serverHostname: originHost, mitmCACertificatePEM: mitmCACertificatePEM, alpn: alpn)
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
            try startTLS(
                on: channel,
                serverHostname: originHost,
                mitmCACertificatePEM: mitmCACertificatePEM,
                applicationProtocols: applicationProtocols,
                expectedALPN: expectedALPN
            )

            completion.advance(to: .request)
            let authority = "\(originHost):\(originPort)"
            if expectedALPN == "h2" {
                try sendHTTP2Request(on: channel, authority: authority, completion: completion)
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
    ) throws {
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
    ) throws {
        let handshake = try installTLS(
            on: channel,
            serverHostname: serverHostname,
            mitmCACertificatePEM: mitmCACertificatePEM,
            applicationProtocols: applicationProtocols
        )
        guard try handshake.wait() == expectedALPN else { throw ProxyTestError.unexpectedALPN }
    }

    private func installTLS(
        on channel: Channel,
        serverHostname: String,
        mitmCACertificatePEM: String,
        applicationProtocols: [String]
    ) throws -> EventLoopFuture<String?> {
        var tls = TLSConfiguration.makeClientConfiguration()
        tls.applicationProtocols = applicationProtocols
        tls.certificateVerification = .fullVerification
        let trustRoot = try NIOSSLCertificate(bytes: Array(mitmCACertificatePEM.utf8), format: .pem)
        tls.trustRoots = .certificates([trustRoot])
        let sslContext = try NIOSSLContext(configuration: tls)
        let handshake = channel.eventLoop.makePromise(of: String?.self)
        let installFuture = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandler(
                NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname),
                position: .first
            )
            try channel.pipeline.syncOperations.addHandler(TLSHandshakeProbe(promise: handshake))
        }
        try installFuture.wait()
        return handshake.futureResult
    }

    private func sendHTTP2Request(
        on channel: Channel,
        authority: String,
        completion: ProxyFetchCompletion
    ) throws {
        let muxFuture = channel.configureHTTP2Pipeline(
            mode: .client,
            connectionConfiguration: .init(),
            streamConfiguration: .init()
        ) { $0.close() }
        let mux = try muxFuture.wait()
        let streamFuture = mux.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(
                    H2ConsumingClient(authority: authority, completion: completion)
                )
            }
        }
        _ = try streamFuture.wait()
    }
}

private final class TLSHandshakeProbe: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let promise: EventLoopPromise<String?>
    private var completed = false

    init(promise: EventLoopPromise<String?>) {
        self.promise = promise
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let negotiatedProtocol) = event {
            complete(.success(negotiatedProtocol))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        complete(.failure(ProxyTestError.tlsClosedBeforeHandshake))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        complete(.failure(error))
        context.close(promise: nil)
    }

    private func complete(_ result: Result<String?, Error>) {
        guard !completed else { return }
        completed = true
        promise.completeWith(result)
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

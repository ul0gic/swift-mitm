import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

enum WebSocketFixtureError: Error {
    case clientClosedBeforeExchangeCompleted
    case originClosedBeforeExchangeCompleted
    case timeout
    case missingPeerCertificate
    case unexpectedALPN
    case unexpectedClientBytes
    case unexpectedOriginBytes
    case unexpectedResponseHead
}

final class OneShot<Value: Sendable>: @unchecked Sendable {
    let futureResult: EventLoopFuture<Value>

    private let claimed = NIOLockedValueBox(false)
    private let promise: EventLoopPromise<Value>

    init(eventLoop: EventLoop) {
        promise = eventLoop.makePromise()
        futureResult = promise.futureResult
    }

    func complete(_ result: Result<Value, Error>) {
        let shouldComplete = claimed.withLockedValue { value in
            guard !value else { return false }
            value = true
            return true
        }
        if shouldComplete {
            promise.completeWith(result)
        }
    }
}

final class WebSocketTLSOriginServer: @unchecked Sendable {
    let caCertificatePEM: String

    private let group: EventLoopGroup
    private let sslContext: NIOSSLContext
    private let completion: OneShot<WebSocketOriginResult>
    private let targetHost: String
    private let activeChildren = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup, targetHost: String) throws {
        self.group = group
        self.targetHost = targetHost
        let ca = try CertificateAuthority(commonName: "SwiftMITM WebSocket Origin Root")
        caCertificatePEM = ca.caCertificatePEM
        let leaf = try ca.leaf(forHost: targetHost)
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: leaf.certificateChain,
            privateKey: leaf.privateKey
        )
        configuration.applicationProtocols = ["http/1.1"]
        sslContext = try NIOSSLContext(configuration: configuration)
        completion = OneShot(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }

    func start() throws {
        let sslContext = sslContext
        let completion = completion
        let activeChildren = activeChildren
        let targetHost = targetHost
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                activeChildren.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    activeChildren.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers([
                        NIOSSLServerHandler(context: sslContext),
                        WebSocketOriginHandler(targetHost: targetHost, completion: completion)
                    ])
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func waitForResult() throws -> WebSocketOriginResult {
        try completion.futureResult.wait()
    }

    func stop() {
        try? channel?.close().wait()
        activeChildren.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
    }
}

struct WebSocketOriginResult: Sendable {
    let requestHead: [UInt8]
    let frames: [UInt8]
}

private final class WebSocketOriginHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let completion: OneShot<WebSocketOriginResult>
    private let targetHost: String
    private var requestBytes: [UInt8] = []
    private var requestHead: [UInt8]?
    private var frameBytes: [UInt8] = []
    private var sentDataFrame = false
    private var sentCloseFrame = false

    init(targetHost: String, completion: OneShot<WebSocketOriginResult>) {
        self.targetHost = targetHost
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let bytes = Array(unwrapInboundIn(data).readableBytesView)
        if requestHead == nil {
            requestBytes.append(contentsOf: bytes)
            consumeRequestHead(context: context)
        } else {
            frameBytes.append(contentsOf: bytes)
            consumeFrames(context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(WebSocketFixtureError.originClosedBeforeExchangeCompleted))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func consumeRequestHead(context: ChannelHandlerContext) {
        let delimiter: [UInt8] = [13, 10, 13, 10]
        guard let range = requestBytes.firstRange(of: delimiter) else { return }
        let end = range.upperBound
        let head = Array(requestBytes[..<end])
        guard head == WebSocketWire.requestHead(
            originHost: targetHost,
            originPort: context.channel.localAddress?.port ?? 0
        ) else {
            completion.complete(.failure(WebSocketFixtureError.unexpectedOriginBytes))
            context.close(promise: nil)
            return
        }
        requestHead = head
        frameBytes.append(contentsOf: requestBytes[end...])
        requestBytes.removeAll()
        var response = context.channel.allocator.buffer(capacity: WebSocketWire.responseHead.count)
        response.writeBytes(WebSocketWire.responseHead)
        context.writeAndFlush(wrapOutboundOut(response), promise: nil)
        consumeFrames(context: context)
    }

    private func consumeFrames(context: ChannelHandlerContext) {
        let expected = WebSocketWire.clientFrames
        guard frameBytes.count <= expected.count, frameBytes == Array(expected.prefix(frameBytes.count)) else {
            completion.complete(.failure(WebSocketFixtureError.unexpectedClientBytes))
            context.close(promise: nil)
            return
        }
        if frameBytes.count >= WebSocketWire.clientTextFrame.count, !sentDataFrame {
            sentDataFrame = true
            var frame = context.channel.allocator.buffer(capacity: WebSocketWire.serverBinaryFrame.count)
            frame.writeBytes(WebSocketWire.serverBinaryFrame)
            context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
        }
        guard frameBytes.count == expected.count, !sentCloseFrame, let requestHead else { return }
        sentCloseFrame = true
        completion.complete(.success(WebSocketOriginResult(requestHead: requestHead, frames: frameBytes)))
        var close = context.channel.allocator.buffer(capacity: WebSocketWire.serverCloseFrame.count)
        close.writeBytes(WebSocketWire.serverCloseFrame)
        let channel = context.channel
        context.writeAndFlush(wrapOutboundOut(close)).whenComplete { _ in channel.close(promise: nil) }
    }
}

final class WebSocketProxyClient {
    private let group: EventLoopGroup

    init(group: EventLoopGroup) {
        self.group = group
    }

    func exchange(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String
    ) throws -> WebSocketClientResult {
        let clientCompletion = OneShot<WebSocketClientResult>(eventLoop: group.next())
        let timeout = clientCompletion.futureResult.eventLoop.scheduleTask(in: .seconds(10)) {
            clientCompletion.complete(.failure(WebSocketFixtureError.timeout))
        }
        clientCompletion.futureResult.whenComplete { _ in timeout.cancel() }

        let testClient = ProxyTestClient(group: group)
        let channel = try testClient.openTunnel(
            proxyPort: proxyPort,
            originHost: originHost,
            originPort: originPort
        )
        defer { try? channel.close().wait() }
        let peerCertificateDER = try startTLS(
            on: channel,
            originHost: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM
        )
        let installation = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandler(
                WebSocketClientHandler(
                    originHost: originHost,
                    originPort: originPort,
                    peerCertificateDER: peerCertificateDER,
                    completion: clientCompletion
                )
            )
        }
        try installation.wait()

        let clientResult = try clientCompletion.futureResult.wait()
        try channel.closeFuture.wait()
        return clientResult
    }

    private func startTLS(
        on channel: Channel,
        originHost: String,
        mitmCACertificatePEM: String
    ) throws -> [UInt8] {
        var tls = TLSConfiguration.makeClientConfiguration()
        tls.applicationProtocols = ["http/1.1"]
        tls.certificateVerification = .fullVerification
        let trustRoot = try NIOSSLCertificate(bytes: Array(mitmCACertificatePEM.utf8), format: .pem)
        tls.trustRoots = .certificates([trustRoot])
        let context = try NIOSSLContext(configuration: tls)
        let serverHostname = (try? SocketAddress(ipAddress: originHost, port: 0)) == nil ? originHost : nil
        let handshake = OneShot<String?>(eventLoop: channel.eventLoop)
        let installation = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandler(
                NIOSSLClientHandler(context: context, serverHostname: serverHostname),
                position: .first
            )
            try channel.pipeline.syncOperations.addHandler(WebSocketTLSHandshakeProbe(completion: handshake))
        }
        try installation.wait()
        guard try handshake.futureResult.wait() == "http/1.1" else {
            throw WebSocketFixtureError.unexpectedALPN
        }
        guard let certificate = try channel.nioSSL_peerCertificate().wait() else {
            throw WebSocketFixtureError.missingPeerCertificate
        }
        return try certificate.toDERBytes()
    }
}

struct WebSocketClientResult: Sendable {
    let responseHead: [UInt8]
    let frames: [UInt8]
    let peerCertificateDER: [UInt8]
}

private final class WebSocketClientHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let originHost: String
    private let originPort: Int
    private let peerCertificateDER: [UInt8]
    private let completion: OneShot<WebSocketClientResult>
    private var responseBytes: [UInt8] = []
    private var responseHead: [UInt8]?
    private var frameBytes: [UInt8] = []
    private var sentRequest = false
    private var sentTextFrame = false
    private var sentCloseFrame = false

    init(
        originHost: String,
        originPort: Int,
        peerCertificateDER: [UInt8],
        completion: OneShot<WebSocketClientResult>
    ) {
        self.originHost = originHost
        self.originPort = originPort
        self.peerCertificateDER = peerCertificateDER
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

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let bytes = Array(unwrapInboundIn(data).readableBytesView)
        if responseHead == nil {
            responseBytes.append(contentsOf: bytes)
            consumeResponseHead(context: context)
        } else {
            frameBytes.append(contentsOf: bytes)
            consumeFrames(context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if frameBytes != WebSocketWire.serverFrames {
            completion.complete(.failure(WebSocketFixtureError.clientClosedBeforeExchangeCompleted))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func sendRequest(context: ChannelHandlerContext) {
        guard !sentRequest else { return }
        sentRequest = true
        let request = WebSocketWire.requestHead(originHost: originHost, originPort: originPort)
        var buffer = context.channel.allocator.buffer(capacity: request.count)
        buffer.writeBytes(request)
        context.writeAndFlush(wrapOutboundOut(buffer), promise: nil)
    }

    private func consumeResponseHead(context: ChannelHandlerContext) {
        let delimiter: [UInt8] = [13, 10, 13, 10]
        guard let range = responseBytes.firstRange(of: delimiter) else { return }
        let end = range.upperBound
        let head = Array(responseBytes[..<end])
        guard head == WebSocketWire.responseHead else {
            completion.complete(.failure(WebSocketFixtureError.unexpectedResponseHead))
            context.close(promise: nil)
            return
        }
        responseHead = head
        frameBytes.append(contentsOf: responseBytes[end...])
        responseBytes.removeAll()
        sendTextFrame(context: context)
        consumeFrames(context: context)
    }

    private func consumeFrames(context: ChannelHandlerContext) {
        let expected = WebSocketWire.serverFrames
        guard frameBytes.count <= expected.count, frameBytes == Array(expected.prefix(frameBytes.count)) else {
            completion.complete(.failure(WebSocketFixtureError.unexpectedOriginBytes))
            context.close(promise: nil)
            return
        }
        if frameBytes.count >= WebSocketWire.serverBinaryFrame.count, !sentCloseFrame {
            sentCloseFrame = true
            var close = context.channel.allocator.buffer(capacity: WebSocketWire.clientCloseFrame.count)
            close.writeBytes(WebSocketWire.clientCloseFrame)
            context.writeAndFlush(wrapOutboundOut(close), promise: nil)
        }
        guard frameBytes.count == expected.count, let responseHead else { return }
        completion.complete(.success(WebSocketClientResult(
            responseHead: responseHead,
            frames: frameBytes,
            peerCertificateDER: peerCertificateDER
        )))
    }

    private func sendTextFrame(context: ChannelHandlerContext) {
        guard !sentTextFrame else { return }
        sentTextFrame = true
        var frame = context.channel.allocator.buffer(capacity: WebSocketWire.clientTextFrame.count)
        frame.writeBytes(WebSocketWire.clientTextFrame)
        context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
    }
}

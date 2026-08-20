import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL

@testable import SwiftMITM

struct RejectedWebSocketExchange: Sendable {
    let originRequestHeads: [[UInt8]]
    let clientResponseBytes: [UInt8]
}

final class RejectedWebSocketTLSOriginServer: @unchecked Sendable {
    let caCertificatePEM: String

    private let group: EventLoopGroup
    private let sslContext: NIOSSLContext
    private let completion: OneShot<[[UInt8]]>
    private let activeChildren = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup) throws {
        self.group = group
        let ca = try CertificateAuthority(commonName: "SwiftMITM Rejected Upgrade Origin Root")
        caCertificatePEM = ca.caCertificatePEM
        let leaf = try ca.leaf(forHost: "localhost")
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
                        RejectedWebSocketOriginHandler(completion: completion)
                    ])
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func waitForRequestHeads() throws -> [[UInt8]] {
        try completion.futureResult.wait()
    }

    func stop() {
        try? channel?.close().wait()
        activeChildren.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
    }
}

final class RejectedWebSocketProxyClient {
    private let group: EventLoopGroup

    init(group: EventLoopGroup) {
        self.group = group
    }

    func exchange(
        proxyPort: Int,
        originPort: Int,
        mitmCACertificatePEM: String
    ) throws -> [UInt8] {
        let completion = OneShot<[UInt8]>(eventLoop: group.next())
        let timeout = completion.futureResult.eventLoop.scheduleTask(in: .seconds(10)) {
            completion.complete(.failure(WebSocketFixtureError.timeout))
        }
        completion.futureResult.whenComplete { _ in timeout.cancel() }
        let client = ProxyTestClient(group: group)
        let channel = try client.openTunnel(proxyPort: proxyPort, originHost: "localhost", originPort: originPort)
        defer { try? channel.close().wait() }
        let tls = try client.installTLS(
            on: channel,
            serverHostname: "localhost",
            mitmCACertificatePEM: mitmCACertificatePEM,
            applicationProtocols: ["http/1.1"]
        )
        try tls.installed.wait()
        guard try tls.handshake.wait() == "http/1.1" else { throw WebSocketFixtureError.unexpectedALPN }
        let installation = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandler(
                RejectedWebSocketClientHandler(originPort: originPort, completion: completion)
            )
        }
        try installation.wait()
        return try completion.futureResult.wait()
    }
}

private final class RejectedWebSocketOriginHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let completion: OneShot<[[UInt8]]>
    private var bytes: [UInt8] = []
    private var requestHeads: [[UInt8]] = []

    init(completion: OneShot<[[UInt8]]>) {
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        bytes.append(contentsOf: unwrapInboundIn(data).readableBytesView)
        consumeRequests(context: context)
    }

    func channelInactive(context: ChannelHandlerContext) {
        if requestHeads.count != 2 {
            completion.complete(.failure(WebSocketFixtureError.originClosedBeforeExchangeCompleted))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func consumeRequests(context: ChannelHandlerContext) {
        let delimiter: [UInt8] = [13, 10, 13, 10]
        while let range = bytes.firstRange(of: delimiter) {
            let requestHead = Array(bytes[..<range.upperBound])
            bytes.removeFirst(range.upperBound)
            requestHeads.append(requestHead)
            switch requestHeads.count {
            case 1:
                write(Self.rejectionResponse, context: context)
            case 2:
                completion.complete(.success(requestHeads))
                write(Self.successResponse, context: context)
            default:
                completion.complete(.failure(WebSocketFixtureError.unexpectedOriginBytes))
                context.close(promise: nil)
            }
        }
    }

    private func write(_ bytes: [UInt8], context: ChannelHandlerContext) {
        context.writeAndFlush(NIOAny(ByteBuffer(bytes: bytes)), promise: nil)
    }

    fileprivate static let rejectionResponse = Array(
        "HTTP/1.1 403 Forbidden\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\n".utf8
    ) + [0x88, 0x00]
    fileprivate static let successResponse = Array("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".utf8)
}

final class RejectedWebSocketClientHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let originPort: Int
    private let completion: OneShot<[UInt8]>
    private var responseBytes: [UInt8] = []
    private var sentUpgrade = false
    private var sentFollowUp = false

    init(originPort: Int, completion: OneShot<[UInt8]>) {
        self.originPort = originPort
        self.completion = completion
    }

    func handlerAdded(context: ChannelHandlerContext) {
        if context.channel.isActive {
            sendUpgrade(context: context)
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        responseBytes.append(contentsOf: unwrapInboundIn(data).readableBytesView)
        if !sentFollowUp, responseBytes.count >= Self.rejectionResponse.count {
            guard Array(responseBytes.prefix(Self.rejectionResponse.count)) == Self.rejectionResponse else {
                fail(WebSocketFixtureError.unexpectedResponseHead, context: context)
                return
            }
            sentFollowUp = true
            write(Self.followUpRequest(originPort: originPort), context: context)
        }
        let expected = Self.rejectionResponse + Self.successResponse
        guard responseBytes.count <= expected.count, responseBytes == Array(expected.prefix(responseBytes.count)) else {
            fail(WebSocketFixtureError.unexpectedOriginBytes, context: context)
            return
        }
        if responseBytes.count == expected.count {
            completion.complete(.success(responseBytes))
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if responseBytes != Self.rejectionResponse + Self.successResponse {
            completion.complete(.failure(WebSocketFixtureError.clientClosedBeforeExchangeCompleted))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error, context: context)
    }

    private func sendUpgrade(context: ChannelHandlerContext) {
        guard !sentUpgrade else { return }
        sentUpgrade = true
        write(Self.upgradeRequest(originPort: originPort), context: context)
    }

    private func write(_ bytes: [UInt8], context: ChannelHandlerContext) {
        context.writeAndFlush(wrapOutboundOut(ByteBuffer(bytes: bytes)), promise: nil)
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    static func upgradeRequest(originPort: Int) -> [UInt8] {
        Array(
            (
                "GET /socket HTTP/1.1\r\nHost: localhost:\(originPort)\r\n"
                + "Upgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
            ).utf8
        )
    }

    static func followUpRequest(originPort: Int) -> [UInt8] {
        Array("GET /after HTTP/1.1\r\nHost: localhost:\(originPort)\r\nConnection: close\r\n\r\n".utf8)
    }

    static let rejectionResponse = RejectedWebSocketOriginHandler.rejectionResponse
    static let successResponse = RejectedWebSocketOriginHandler.successResponse
}

import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

final class TLSOriginServer {
    let ca: CertificateAuthority
    let hostname = "localhost"

    private let group: EventLoopGroup
    private let bodySize: Int
    private let applicationProtocols: [String]
    private let performsTLSHandshake: Bool
    private let activeChildren = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(
        group: EventLoopGroup,
        bodySize: Int,
        applicationProtocols: [String] = ["h2", "http/1.1"],
        performsTLSHandshake: Bool = true
    ) throws {
        self.group = group
        self.bodySize = bodySize
        self.applicationProtocols = applicationProtocols
        self.performsTLSHandshake = performsTLSHandshake
        self.ca = try CertificateAuthority(commonName: "SwiftMITM Test Origin Root")
    }

    var caCertificatePEM: String { ca.caCertificatePEM }
    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var activeChildChannels: [Channel] { activeChildren.withLockedValue { Array($0.values) } }

    func start() throws {
        let leaf = try ca.leaf(forHost: hostname)
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: leaf.certificateChain,
            privateKey: leaf.privateKey
        )
        configuration.applicationProtocols = applicationProtocols
        let sslContext = try NIOSSLContext(configuration: configuration)
        let bodySize = bodySize
        let activeChildren = activeChildren
        let performsTLSHandshake = performsTLSHandshake

        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                activeChildren.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    activeChildren.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                guard performsTLSHandshake else {
                    return channel.eventLoop.makeSucceededVoidFuture()
                }
                return channel.eventLoop.makeCompletedFuture {
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
        activeChildChannels.forEach { try? $0.close().wait() }
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
            channel.eventLoop.makeCompletedFuture {
                try channel.pipeline.syncOperations.addHandler(OriginHTTP1Responder(bodySize: bodySize))
            }
        }
    }
}

private final class OriginHTTP1Responder: ChannelDuplexHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundIn = HTTPServerResponsePart
    typealias OutboundOut = HTTPServerResponsePart

    private var remaining: Int
    private let chunkSize = 64 * 1024
    private var responseStarted = false
    private var writeInFlight = false
    private var finished = false

    init(bodySize: Int) {
        self.remaining = bodySize
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case .end = unwrapInboundIn(data), !responseStarted else { return }
        responseStarted = true
        var headers = HTTPHeaders()
        headers.add(name: "content-length", value: String(remaining))
        headers.add(name: "content-type", value: "application/octet-stream")
        let head = HTTPResponseHead(version: .http1_1, status: .ok, headers: headers)
        context.writeAndFlush(wrapOutboundOut(.head(head)), promise: nil)
        pump(context: context)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        if context.channel.isWritable {
            pump(context: context)
        }
        context.fireChannelWritabilityChanged()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }

    private func pump(context: ChannelHandlerContext) {
        guard !writeInFlight, context.channel.isWritable else { return }
        guard remaining > 0 else {
            guard !finished else { return }
            finished = true
            context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
            return
        }
        let count = min(chunkSize, remaining)
        var buffer = context.channel.allocator.buffer(capacity: count)
        buffer.writeRepeatingByte(0x41, count: count)
        remaining -= count
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
                handler.pump(context: context)
            case .failure:
                context.close(promise: nil)
            }
        }
        context.writeAndFlush(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: writePromise)
    }
}

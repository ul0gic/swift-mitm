import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

final class OriginState: Sendable {
    struct Snapshot: Sendable {
        var protocols: [String] = []
        var paths: [String] = []
    }

    private let storage = NIOLockedValueBox(Snapshot())
    let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])

    func negotiated(_ value: String) { storage.withLockedValue { $0.protocols.append(value) } }
    func requested(_ value: String) { storage.withLockedValue { $0.paths.append(value) } }
    func snapshot() -> Snapshot { storage.withLockedValue { $0 } }
}

func startOrigin(
    group: EventLoopGroup,
    directory: String,
    protocolName: String,
    state: OriginState
) async throws -> Channel {
    let root = directory + "/origin/origin-ca.pem"
    let key = directory + "/origin/origin-key.pem"
    var configuration = TLSConfiguration.makeServerConfiguration(
        certificateChain: try NIOSSLCertificate.fromPEMFile(root).map { .certificate($0) },
        privateKey: .privateKey(try NIOSSLPrivateKey(file: key, format: .pem))
    )
    configuration.applicationProtocols = [protocolName]
    let tls = try NIOSSLContext(configuration: configuration)
    return try await ServerBootstrap(group: group)
        .childChannelInitializer { channel in
            let identifier = ObjectIdentifier(channel)
            state.children.withLockedValue { $0[identifier] = channel }
            channel.closeFuture.whenComplete { _ in
                state.children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
            }
            return channel.eventLoop.makeCompletedFuture {
                try channel.pipeline.syncOperations.addHandlers([
                    NIOSSLServerHandler(context: tls),
                    ApplicationProtocolNegotiationHandler { result, channel in
                        guard case let .negotiated(selected) = result, selected == protocolName else {
                            return channel.eventLoop.makeFailedFuture(MeasurementError.protocolMismatch)
                        }
                        state.negotiated(protocolName)
                        return configureOrigin(channel, protocolName: protocolName, state: state)
                    }
                ])
            }
        }
        .bind(host: "127.0.0.1", port: 0)
        .get()
}

private func configureOrigin(
    _ channel: Channel,
    protocolName: String,
    state: OriginState
) -> EventLoopFuture<Void> {
    if protocolName == "h2" {
        let pipeline = channel.configureHTTP2Pipeline(
            mode: .server,
            connectionConfiguration: .init(),
            streamConfiguration: .init()
        ) { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandlers([
                    HTTP2FramePayloadToHTTP1ServerCodec(),
                    OriginResponder(state: state)
                ])
            }
        }
        return pipeline.map { _ in () }
    }
    return channel.pipeline.configureHTTPServerPipeline().flatMap {
        channel.eventLoop.makeCompletedFuture {
            try channel.pipeline.syncOperations.addHandler(OriginResponder(state: state))
        }
    }
}

private final class OriginResponder: ChannelDuplexHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundIn = HTTPServerResponsePart
    typealias OutboundOut = HTTPServerResponsePart

    private let state: OriginState
    private var path: String?
    private var remaining = 0
    private var writing = false
    private var responding = false

    init(state: OriginState) { self.state = state }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case let .head(head): path = head.uri
        case .end:
            guard let path, !responding else {
                context.close(promise: nil)
                return
            }
            state.requested(path)
            remaining = path.hasPrefix("/throughput/") ? 268_435_456 : 0
            responding = true
            let head = HTTPResponseHead(
                version: .http1_1,
                status: .ok,
                headers: HTTPHeaders([("content-length", String(remaining))])
            )
            context.write(wrapOutboundOut(.head(head)), promise: nil)
            pump(context)
        case .body: context.close(promise: nil)
        }
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        pump(context)
        context.fireChannelWritabilityChanged()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { context.close(promise: nil) }

    private func pump(_ context: ChannelHandlerContext) {
        guard responding, !writing, context.channel.isWritable else { return }
        guard remaining > 0 else {
            responding = false
            path = nil
            context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
            return
        }
        let count = min(16_384, remaining)
        var buffer = context.channel.allocator.buffer(capacity: count)
        buffer.writeRepeatingByte(0x41, count: count)
        remaining -= count
        writing = true
        let promise = context.eventLoop.makePromise(of: Void.self)
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
        promise.futureResult.whenComplete { result in
            owner.value.writing = false
            switch result {
            case .success: owner.value.pump(boundContext.value)
            case .failure: boundContext.value.close(promise: nil)
            }
        }
        context.writeAndFlush(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: promise)
    }
}

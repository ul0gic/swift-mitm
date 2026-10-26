import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

struct Phase5GuestStyleForwardingObservation: Sendable {
    let source: Phase5ProxyV2Endpoint
    let destination: Phase5ProxyV2Endpoint
    let headerBytes: [UInt8]
}

final class Phase5GuestStyleForwarder {
    private let group: EventLoopGroup
    private let proxyPort: Int
    private let destination: Phase5ProxyV2Endpoint
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private let observationCompletion: Phase2FixtureCompletion<Phase5GuestStyleForwardingObservation>
    private var listener: Channel?

    init(group: EventLoopGroup, proxyPort: Int, destination: Phase5ProxyV2Endpoint) {
        self.group = group
        self.proxyPort = proxyPort
        self.destination = destination
        observationCompletion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { listener?.localAddress?.port ?? 0 }
    var observation: EventLoopFuture<Phase5GuestStyleForwardingObservation> {
        observationCompletion.futureResult
    }

    func start() throws {
        let children = children
        let destination = destination
        let observationCompletion = observationCompletion
        let proxyPort = proxyPort
        listener = try phase4BoundedWait(ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.autoRead, value: false)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .childChannelInitializer { downstream in
                Self.configure(
                    downstream,
                    proxyPort: proxyPort,
                    destination: destination,
                    children: children,
                    observationCompletion: observationCompletion
                )
            }
            .bind(host: "127.0.0.1", port: 0))
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? phase4BoundedWait($0.close()) }
        if let listener {
            try? phase4BoundedWait(listener.close())
        }
        observationCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }

    private static func configure(
        _ downstream: Channel,
        proxyPort: Int,
        destination: Phase5ProxyV2Endpoint,
        children: NIOLockedValueBox<[ObjectIdentifier: Channel]>,
        observationCompletion: Phase2FixtureCompletion<Phase5GuestStyleForwardingObservation>
    ) -> EventLoopFuture<Void> {
        register(downstream, in: children)
        guard let remoteAddress = downstream.remoteAddress,
              let source = Phase5ProxyV2Endpoint(socketAddress: remoteAddress),
              let headerBytes = try? Phase5ProxyV2Encoder.encode(source: source, destination: destination) else {
            return downstream.eventLoop.makeFailedFuture(Phase5ProxyV2ConformanceError.invalidEndpoint)
        }
        return ClientBootstrap(group: downstream.eventLoop)
            .connectTimeout(.seconds(2))
            .channelOption(ChannelOptions.autoRead, value: false)
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .connect(host: "127.0.0.1", port: proxyPort)
            .flatMap { upstream in
                register(upstream, in: children)
                return installRelay(downstream: downstream, upstream: upstream)
                    .flatMap { upstream.writeAndFlush(ByteBuffer(bytes: headerBytes)) }
                    .map {
                        observationCompletion.complete(.success(.init(
                            source: source,
                            destination: destination,
                            headerBytes: headerBytes
                        )))
                        downstream.read()
                        upstream.read()
                    }
                    .flatMapError { error in
                        upstream.close(promise: nil)
                        return upstream.eventLoop.makeFailedFuture(error)
                    }
            }
            .flatMapError { error in
                observationCompletion.complete(.failure(error))
                downstream.close(promise: nil)
                return downstream.eventLoop.makeFailedFuture(error)
            }
    }

    private static func installRelay(downstream: Channel, upstream: Channel) -> EventLoopFuture<Void> {
        upstream.eventLoop.makeCompletedFuture {
            let pair = Phase5GuestRelayHandler.matchedPair()
            try downstream.pipeline.syncOperations.addHandler(pair.downstream)
            try upstream.pipeline.syncOperations.addHandler(pair.upstream)
        }
    }

    private static func register(
        _ channel: Channel,
        in children: NIOLockedValueBox<[ObjectIdentifier: Channel]>
    ) {
        let identifier = ObjectIdentifier(channel)
        children.withLockedValue { $0[identifier] = channel }
        channel.closeFuture.whenComplete { _ in
            children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
        }
    }
}

private final class Phase5GuestRelayHandler {
    private var partner: Phase5GuestRelayHandler?
    private var context: ChannelHandlerContext?
    private var pendingRead = false
    private var writable = false
    private var inputEnded = false

    static func matchedPair() -> (downstream: Phase5GuestRelayHandler, upstream: Phase5GuestRelayHandler) {
        let downstream = Phase5GuestRelayHandler()
        let upstream = Phase5GuestRelayHandler()
        downstream.partner = upstream
        upstream.partner = downstream
        return (downstream, upstream)
    }

    private func updateWritability(_ writable: Bool) {
        self.writable = writable
        if writable {
            partner?.resumeRead()
        }
    }

    private func resumeRead() {
        guard pendingRead else { return }
        pendingRead = false
        context?.read()
    }

    private var partnerWritable: Bool { partner?.writable ?? false }
}

extension Phase5GuestRelayHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        updateWritability(context.channel.isWritable)
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        writable = false
        self.context = nil
        partner = nil
    }

    func channelActive(context: ChannelHandlerContext) {
        updateWritability(context.channel.isWritable)
        context.fireChannelActive()
        if partnerWritable {
            context.read()
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard let partnerContext = partner?.context else {
            context.close(promise: nil)
            return
        }
        partnerContext.write(NIOAny(unwrapInboundIn(data)), promise: nil)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        partner?.context?.flush()
        if partnerWritable {
            context.read()
        } else {
            pendingRead = true
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        partner?.context?.close(promise: nil)
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            guard !inputEnded else { return }
            inputEnded = true
            partner?.context?.close(mode: .output, promise: nil)
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
        partner?.context?.close(promise: nil)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        updateWritability(context.channel.isWritable)
        context.fireChannelWritabilityChanged()
    }

    func read(context: ChannelHandlerContext) {
        if partnerWritable {
            context.read()
        } else {
            pendingRead = true
        }
    }
}

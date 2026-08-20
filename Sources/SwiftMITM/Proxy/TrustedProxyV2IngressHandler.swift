import NIOCore

struct TrustedProxyV2AcceptedConnection: Sendable {
    let metadata: ProxyV2Metadata
    let target: ConnectionTarget
}

enum TrustedProxyV2IngressError: Error, Equatable, Sendable {
    case peerAddressUnavailable
    case untrustedPeer
    case targetUnavailable
    case deadlineExceeded
}

final class TrustedProxyV2IngressHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias PeerAddressProvider = (Channel) -> SocketAddress?
    typealias AcceptanceHandler = (ChannelHandlerContext, TrustedProxyV2AcceptedConnection) throws -> Void

    private let configuration: TrustedProxyV2Ingress
    private let peerAddressProvider: PeerAddressProvider
    private let acceptanceHandler: AcceptanceHandler
    private let stageObserver: (any TransparentIngressStageObserver)?
    private var parser: ProxyV2Parser
    private var deadlineTask: Scheduled<Void>?
    private var pendingStageObserved = false
    private var terminal = false

    init(
        configuration: TrustedProxyV2Ingress,
        peerAddressProvider: @escaping PeerAddressProvider = { $0.remoteAddress },
        stageObserver: (any TransparentIngressStageObserver)? = nil,
        acceptanceHandler: @escaping AcceptanceHandler
    ) {
        self.configuration = configuration
        self.peerAddressProvider = peerAddressProvider
        self.stageObserver = stageObserver
        self.acceptanceHandler = acceptanceHandler
        parser = ProxyV2Parser(maximumHeaderBytes: configuration.proxyHeaderMaximumBytes)
    }

    func handlerAdded(context: ChannelHandlerContext) {
        guard let peerAddress = peerAddressProvider(context.channel) else {
            return fail(TrustedProxyV2IngressError.peerAddressUnavailable, context: context)
        }
        guard configuration.trustedPeers.admits(peerAddress) else {
            return fail(TrustedProxyV2IngressError.untrustedPeer, context: context)
        }
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
        deadlineTask = context.eventLoop.scheduleTask(in: configuration.proxyHeaderDeadlineTimeAmount) {
            owner.value.fail(TrustedProxyV2IngressError.deadlineExceeded, context: boundContext.value)
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        deadlineTask?.cancel()
        deadlineTask = nil
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !terminal else { return }
        var input = unwrapInboundIn(data)
        do {
            switch try parser.parse(&input) {
            case .pending:
                if !pendingStageObserved {
                    pendingStageObserved = true
                    stageObserver?.didEnterTransparentIngressStage(.proxyHeaderPending)
                }
                return
            case .complete(let metadata):
                try accept(metadata, remainingInput: input, context: context)
            }
        } catch {
            fail(error, context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !terminal {
            do {
                try parser.finish()
            } catch {
                terminal = true
                deadlineTask?.cancel()
                context.fireErrorCaught(error)
            }
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        guard !terminal else { return }
        terminal = true
        deadlineTask?.cancel()
        context.fireErrorCaught(error)
        context.close(promise: nil)
    }

    private func accept(
        _ metadata: ProxyV2Metadata,
        remainingInput: ByteBuffer,
        context: ChannelHandlerContext
    ) throws {
        guard let target = ConnectionTarget(proxyV2Metadata: metadata) else {
            throw TrustedProxyV2IngressError.targetUnavailable
        }
        deadlineTask?.cancel()
        try acceptanceHandler(context, TrustedProxyV2AcceptedConnection(metadata: metadata, target: target))
        terminal = true
        if remainingInput.readableBytes > 0 {
            context.fireChannelRead(wrapInboundOut(remainingInput))
        }
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        guard !terminal else { return }
        terminal = true
        deadlineTask?.cancel()
        context.fireErrorCaught(error)
        context.close(promise: nil)
    }
}

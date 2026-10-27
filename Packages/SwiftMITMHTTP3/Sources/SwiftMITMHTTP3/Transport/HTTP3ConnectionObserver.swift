import NIOCore
import NIOHTTP3

final class HTTP3ConnectionObserver: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    private let ownership: HTTP3ConnectionOwnership
    private let receivedGoaway: () -> Void
    private let failed: (HTTP3ProxyError) -> Void

    init(
        ownership: HTTP3ConnectionOwnership,
        receivedGoaway: @escaping () -> Void,
        failed: @escaping (HTTP3ProxyError) -> Void
    ) {
        self.ownership = ownership
        self.receivedGoaway = receivedGoaway
        self.failed = failed
    }

    func channelActive(context: ChannelHandlerContext) {
        ownership.finishHandshake()
        context.fireChannelActive()
    }

    func channelInactive(context: ChannelHandlerContext) {
        ownership.finishHandshake()
        context.fireChannelInactive()
    }

    func handlerRemoved(context: ChannelHandlerContext) { ownership.finishHandshake() }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        ownership.fail(error)
        failed(ownership.failure ?? .protocolViolation)
        context.close(promise: nil)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is ReceivedGoawayEvent {
            receivedGoaway()
        }
        context.fireUserInboundEventTriggered(event)
    }
}

import NIOCore

final class ProbeHandshakeDeadline: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    private var deadline: Scheduled<Void>?

    func handlerAdded(context: ChannelHandlerContext) {
        let channel = context.channel
        deadline = context.eventLoop.scheduleTask(in: .seconds(10)) {
            channel.pipeline.fireErrorCaught(ProbeError.handshakeTimeout)
            channel.close(promise: nil)
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        deadline?.cancel()
        deadline = nil
        context.fireChannelActive()
    }

    func channelInactive(context: ChannelHandlerContext) {
        deadline?.cancel()
        deadline = nil
        context.fireChannelInactive()
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        deadline?.cancel()
        deadline = nil
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }
}

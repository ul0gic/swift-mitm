import NIOCore

final class HTTPForwardWebSocketTap: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    private let session: WebSocketCaptureSession
    private let direction: WebSocketDirection

    init(session: WebSocketCaptureSession, direction: WebSocketDirection) {
        self.session = session
        self.direction = direction
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        session.capture(unwrapInboundIn(data).readableBytesView, direction: direction)
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        session.close()
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        session.close()
        context.fireErrorCaught(error)
    }
}

import NIOCore
import NIOTLS

final class WebSocketTLSHandshakeProbe: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let completion: OneShot<String?>

    init(completion: OneShot<String?>) {
        self.completion = completion
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let negotiatedProtocol) = event {
            completion.complete(.success(negotiatedProtocol))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(WebSocketFixtureError.clientClosedBeforeExchangeCompleted))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

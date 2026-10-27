import NIOCore
import NIOHTTPTypes

final class HTTP3RequestHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPRequestPart
    let exchange: HTTP3Exchange
    init(exchange: HTTP3Exchange) { self.exchange = exchange }

    func channelActive(context: ChannelHandlerContext) {
        exchange.readIfReady(exchange.request)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        exchange.receive(.request(unwrapInboundIn(data)), flow: exchange.request)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        exchange.readComplete(exchange.request)
        context.fireChannelReadComplete()
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        exchange.pump(exchange.response)
        context.fireChannelWritabilityChanged()
    }

    func channelInactive(context: ChannelHandlerContext) {
        exchange.inactive(exchange.request)
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event as? ChannelEvent == .inputClosed {
            exchange.inputClosed(exchange.request)
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { exchange.fail(error) }
}

final class HTTP3ResponseHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPResponsePart
    let exchange: HTTP3Exchange
    init(exchange: HTTP3Exchange) { self.exchange = exchange }

    func channelActive(context: ChannelHandlerContext) {
        exchange.readIfReady(exchange.response)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        exchange.receive(.response(unwrapInboundIn(data)), flow: exchange.response)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        exchange.readComplete(exchange.response)
        context.fireChannelReadComplete()
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        exchange.pump(exchange.request)
        context.fireChannelWritabilityChanged()
    }

    func channelInactive(context: ChannelHandlerContext) {
        exchange.inactive(exchange.response)
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event as? ChannelEvent == .inputClosed {
            exchange.inputClosed(exchange.response)
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { exchange.fail(error) }
}

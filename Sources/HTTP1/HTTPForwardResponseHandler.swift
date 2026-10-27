import NIOCore
import NIOHTTP1
import SwiftMITMHTTP1Codec

final class HTTPForwardResponseHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPClientResponsePart
    private weak var owner: ExplicitHTTPProxyHandler?

    init(owner: ExplicitHTTPProxyHandler) { self.owner = owner }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard let owner, !owner.stopped, owner.upstream === context.channel else { return }
        owner.receive(unwrapInboundIn(data))
    }

    func channelReadComplete(context: ChannelHandlerContext) { owner?.readOriginIfNeeded() }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        owner?.active?.requestFaultWriter?.drain()
        owner?.readClientIfNeeded()
    }

    func channelInactive(context: ChannelHandlerContext) {
        owner?.originFailed(channel: context.channel)
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let error = event as? ProxyConnectionError, error == .serverStopping {
            owner?.cancel()
            return
        }
        if let metadata = event as? BytePreservingHTTP1MessageMetadata {
            owner?.pendingResponseKeepAlive = metadata.isKeepAlive
            return
        }
        if let event = event as? ChannelEvent, case .inputClosed = event {
            owner?.originFailed(channel: context.channel)
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { owner?.originFailed(channel: context.channel) }
}

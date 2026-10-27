import NIOCore
import NIOHTTP2

extension ProxyServer {
    func configurePriorKnowledgeHTTP2(
        _ channel: Channel,
        configuration: HTTP2PriorKnowledgeIngress
    ) -> EventLoopFuture<Void> {
        guard let target = ConnectionTarget(
            explicitConnectAuthority: configuration.authority,
            ingressProvenance: .priorKnowledgeHTTP2
        ) else {
            return channel.eventLoop.makeFailedFuture(ProxyConnectionError.egressBlocked(configuration.authority))
        }
        return channel.setOption(ChannelOptions.autoRead, value: false)
            .flatMap { [self] in connectUpstreamRaw(target: target, on: channel.eventLoop) }
            .flatMap { [self] upstream in
                bridgeHTTP2(
                    inbound: channel,
                    upstream: NegotiatedUpstreamConnection(
                        channel: upstream.channel,
                        applicationProtocol: .http2,
                        alpnHandler: nil
                    ),
                    authority: configuration.authority,
                    target: upstream.target.capturedTarget,
                    validateAuthority: true
                ).flatMapError { error in
                    upstream.channel.close().flatMapThrowing { throw error }
                }
            }
    }
}

final class HTTP2PriorKnowledgeAuthorityHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias InboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private var receivedHead = false

    init(authority: String) { self.authority = authority }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let payload = unwrapInboundIn(data)
        guard !receivedHead else {
            context.fireChannelRead(data)
            return
        }
        guard case let .headers(head) = payload,
              head.headers.first(name: ":scheme") == "http",
              let value = head.headers.first(name: ":authority") else {
            context.close(promise: nil)
            return
        }
        let normalized = ConnectionTarget(
            explicitConnectAuthority: value,
            ingressProvenance: .priorKnowledgeHTTP2
        ) ?? ConnectionTarget(
            explicitConnectAuthority: value + ":80",
            ingressProvenance: .priorKnowledgeHTTP2
        )
        guard normalized?.logicalAuthority == authority else {
            context.close(promise: nil)
            return
        }
        receivedHead = true
        context.fireChannelRead(data)
    }
}

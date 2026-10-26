import NIOCore
import NIOHTTP1
import SwiftMITMHTTP1Codec

extension ExplicitHTTPProxyHandler {
    func establishCONNECT(_ exchange: Exchange) {
        guard !stopped, let context, count == 1 else {
            reject(status: .badRequest, message: "CONNECT cannot follow pipelined requests")
            return
        }
        reusePool?.close(downstreamID: connectionID)
        let head = HTTPResponseHead(version: .http1_1, status: .ok, headers: ["via": "1.1 SwiftMITM"])
        correlator.end(id: exchange.id, request: false)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        let channel = context.channel
        context.writeAndFlush(wrapOutboundOut(.end(nil))).assumeIsolated()
            .flatMap {
                self.stopped = true
                return context.pipeline.syncOperations.removeHandler(context: context)
            }
            .whenComplete { result in
                switch result {
                case .success:
                    self.onCONNECT(channel, exchange.head.uri)
                    if self.inputClosed {
                        channel.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
                    }
                case .failure:
                    channel.close(promise: nil)
                }
            }
    }

    func upgradeWebSocket(_ exchange: Exchange) {
        guard let context, let upstream, exchange.requestEnded else {
            abort(message: "Invalid WebSocket upgrade state")
            return
        }
        let session = WebSocketCaptureSession()
        session.open(
            id: exchange.id,
            sink: sink,
            captureLimit: captureBodyLimit,
            permessageDeflate: exchange.permessageDeflate
        )
        let pair = GlueHandler.matchedPair()
        let channel = context.channel
        channel.closeFuture.whenComplete { _ in upstream.close(promise: nil) }
        upstream.closeFuture.whenComplete { _ in channel.close(promise: nil) }
        do {
            try channel.pipeline.syncOperations.addHandlers([
                HTTPForwardWebSocketTap(session: session, direction: .clientToServer), pair.0
            ])
            try upstream.pipeline.syncOperations.addHandlers([
                HTTPForwardWebSocketTap(session: session, direction: .serverToClient), pair.1
            ])
        } catch {
            session.close()
            abort(message: "WebSocket upgrade pipeline failed")
            return
        }
        stopped = true
        channel.pipeline.syncOperations.removeHandler(context: context).assumeIsolated()
            .flatMap { Self.remove(HTTPForwardResponseHandler.self, from: upstream) }
            .flatMap { Self.remove(BytePreservingHTTPResponseEncoder.self, from: channel) }
            .flatMap { Self.remove(BytePreservingHTTPRequestEncoder.self, from: upstream) }
            .flatMap {
                Self.remove(ByteToMessageHandler<BytePreservingHTTPResponseDecoder>.self, from: upstream)
            }
            .flatMap {
                Self.remove(ByteToMessageHandler<BytePreservingHTTPRequestDecoder>.self, from: channel)
            }
            .flatMap { channel.setOption(ChannelOptions.autoRead, value: true) }
            .flatMap { upstream.setOption(ChannelOptions.autoRead, value: true) }
            .whenFailure { _ in
                session.close()
                channel.close(promise: nil)
                upstream.close(promise: nil)
            }
    }

    static func remove<Handler: RemovableChannelHandler>(
        _ type: Handler.Type,
        from channel: Channel
    ) -> EventLoopFuture<Void> {
        do {
            return try channel.pipeline.syncOperations.removeHandler(
                channel.pipeline.syncOperations.handler(type: type)
            )
        } catch {
            return channel.eventLoop.makeFailedFuture(error)
        }
    }
}

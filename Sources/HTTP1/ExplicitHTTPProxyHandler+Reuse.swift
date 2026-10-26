import NIOCore
import SwiftMITMHTTP1Codec

extension ExplicitHTTPProxyHandler {
    func reuseKey(_ exchange: Exchange) -> HTTP1ReuseKey? {
        guard reusePool != nil, !typedHTTP2, fixedDestination == nil, exchange.controlSnapshot == nil,
              let target = exchange.target, target.scheme == "http" else { return nil }
        let credentials: Set = ["authorization", "proxy-authorization", "cookie"]
        return HTTP1ReuseKey(
            downstreamID: connectionID,
            authority: target.authority,
            scheme: target.scheme,
            credentialScope: Self.fields(exchange.effectiveHead.headers)
                             .filter { credentials.contains($0.name.lowercased()) }
        )
    }

    func takeReusableOrigin(_ exchange: Exchange, target: HTTPForwardTarget) -> Bool {
        guard let key = reuseKey(exchange), let pooled = reusePool?.take(key: key) else { return false }
        let channel = pooled.channel
        guard authorizeReuse(target, channel), let remote = channel.remoteAddress,
              let unresolved = ConnectionTarget(host: target.host, port: target.port, ingressProvenance: .explicitHTTP)
              else {
            channel.close(promise: nil)
            return false
        }
        exchange.completedExchanges = pooled.completedExchanges
        let owner = NIOLoopBound((self, exchange), eventLoop: channel.eventLoop)
        Self.removeIdleGuards(from: channel).assumeIsolated().whenComplete { result in
            let (handler, exchange) = owner.value
            switch result {
            case .success:
                guard channel.isActive, !handler.stopped else {
                    channel.close(promise: nil)
                    return
                }
                handler.configureOrigin(HTTPForwardConnection(
                    channel: channel,
                    target: ResolvedTarget(unresolved: unresolved, connectedAddress: remote).capturedTarget
                ), exchange: exchange)
            case .failure:
                channel.close(promise: nil)
                handler.reject(status: .badGateway, message: "HTTP reuse checkout failed")
            }
        }
        return true
    }

    func finishWithReuse(_ exchange: Exchange) -> Bool {
        guard let pool = reusePool, let key = reuseKey(exchange), let channel = upstream,
              exchange.requestEnded, exchange.responseEnded, exchange.requestKeepAlive,
              exchange.originKeepAlive == true, exchange.reusableFraming, !exchange.earlyResponse,
              !exchange.upgrading, !exchange.reusePoisoned else { return false }
        guard !exchange.finishing else { return true }
        exchange.finishing = true
        let owner = NIOLoopBound((self, exchange), eventLoop: channel.eventLoop)
        channel.eventLoop.execute {
            let (handler, exchange) = owner.value
            guard !handler.stopped, handler.active === exchange else { return }
            guard channel.isActive, !exchange.reusePoisoned,
                  exchange.responseDecoder?.isAtMessageBoundary == true else {
                handler.finishExchange(exchange)
                return
            }
            handler.parkReusableOrigin(channel, exchange: exchange, key: key, pool: pool)
        }
        return true
    }

    func parkReusableOrigin(
        _ channel: Channel,
        exchange: Exchange,
        key: HTTP1ReuseKey,
        pool: HTTP1ReusePool
    ) {
        do {
            try channel.pipeline.syncOperations.addHandler(HTTP1IdleGuard(), name: "http1.reuse.raw", position: .first)
            try channel.pipeline.syncOperations.addHandler(HTTP1IdleGuard(), name: "http1.reuse.typed")
        } catch {
            self.finishExchange(exchange)
            return
        }
        Self.remove(HTTPForwardResponseHandler.self, from: channel)
            .flatMap { Self.remove(ByteToMessageHandler<BytePreservingHTTPResponseDecoder>.self, from: channel) }
            .flatMap { Self.remove(BytePreservingHTTPRequestEncoder.self, from: channel) }
            .assumeIsolated()
.whenComplete { result in
                guard !self.stopped, self.active === exchange else {
                    channel.close(promise: nil)
                    return
                }
                switch result {
                case .success:
                    if channel.isActive, !exchange.reusePoisoned,
                       pool.put(channel: channel, key: key, completedExchanges: exchange.completedExchanges + 1) {
                        self.upstream = nil
                        channel.read()
                    }
                case .failure: channel.close(promise: nil)
                }
                self.finishExchange(exchange)
            }
    }

    static func removeIdleGuards(from channel: Channel) -> EventLoopFuture<Void> {
        do {
            let raw = try channel.pipeline.syncOperations.context(name: "http1.reuse.raw")
            let typed = NIOLoopBound(
                try channel.pipeline.syncOperations.context(name: "http1.reuse.typed"),
                eventLoop: channel.eventLoop
            )
            return channel.pipeline
.syncOperations
.removeHandler(context: raw)
                .flatMap { channel.pipeline.syncOperations.removeHandler(context: typed.value) }
        } catch { return channel.eventLoop.makeFailedFuture(error) }
    }
}

import NIOCore
import NIOPosix
import NIOSSL

extension ProxyServer {
    func enforceSetupDeadline<Value: Sendable>(
        _ future: EventLoopFuture<Value>,
        timeout: TimeAmount,
        setupToken: ProxyConnectionSetupToken,
        cleanup: @escaping @Sendable (Value) -> EventLoopFuture<Void>
    ) -> EventLoopFuture<Value> {
        setupToken.retainOperation()
        let completion = ProxySetupStageCompletion<Value>(eventLoop: future.eventLoop)
        let deadlineTask = future.eventLoop.scheduleTask(in: timeout) {
            if completion.complete(.failure(ProxyConnectionError.setupTimedOut)) {
                setupToken.cancel()
            }
        }
        setupToken.onCancellation(on: future.eventLoop) {
            deadlineTask.cancel()
            completion.complete(.failure(ProxyConnectionError.serverStopping))
        }
        future.whenComplete { result in
            deadlineTask.cancel()
            if setupToken.isCancelled {
                if case .success(let value) = result {
                    cleanup(value).whenComplete { _ in setupToken.releaseOperation() }
                } else {
                    setupToken.releaseOperation()
                }
                completion.complete(.failure(ProxyConnectionError.serverStopping))
            } else {
                completion.complete(result)
                setupToken.releaseOperation()
            }
        }
        return completion.futureResult
    }

    func connectUpstreamTLS(
        target: ConnectionTarget,
        applicationProtocols: [ALPNProtocol],
        tlsRuntime: ProxyTLSRuntime,
        setupToken suppliedSetupToken: ProxyConnectionSetupToken? = nil,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>? = nil,
        on loop: EventLoop
    ) -> EventLoopFuture<ResolvedUpstreamConnection> {
        guard let setupToken = suppliedSetupToken ?? connectionRegistry.beginSetup() else {
            return loop.makeFailedFuture(ProxyConnectionError.serverStopping)
        }
        let context: NIOSSLContext
        do {
            context = try tlsRuntime.upstreamContext(applicationProtocols: applicationProtocols)
        } catch {
            if suppliedSetupToken == nil {
                setupToken.complete()
            }
            return loop.makeFailedFuture(error)
        }
        let egressPolicy = egressPolicy
        failureHandler?.value.updateStage(.routing)
        if egressPolicy.deniesLiteral(target.connectionHost) {
            if suppliedSetupToken == nil {
                setupToken.complete()
            }
            return loop.makeFailedFuture(ProxyConnectionError.egressBlocked(target.connectionHost))
        }
        failureHandler?.value.updateStage(.connect)
        let connect = beginUpstreamTLSConnection(
            target: target,
            egressPolicy: egressPolicy,
            setupToken: setupToken,
            on: loop
        )
        return enforceSetupDeadline(
            connect,
            timeout: timeoutPolicy.upstreamConnectDeadline.nioTimeAmount ?? .seconds(10),
            setupToken: setupToken
        ) { $0.close() }
            .flatMap { [self] channel in
                guard !setupToken.isCancelled else {
                    return channel.close().flatMapThrowing { throw ProxyConnectionError.serverStopping }
                }
                setupToken.onCancellation(on: channel.eventLoop) {
                    channel.close(promise: nil)
                }
                failureHandler?.value.updateStage(.upstreamTLS)
                let negotiation = negotiateUpstreamTLS(
                    on: channel,
                    context: context,
                    target: target,
                    egressPolicy: egressPolicy
                )
                return enforceSetupDeadline(
                    negotiation,
                    timeout: timeoutPolicy.tlsHandshakeDeadline.nioTimeAmount ?? .seconds(10),
                    setupToken: setupToken
                ) { $0.connection.channel.close() }
            }
            .always { _ in
                if suppliedSetupToken == nil {
                    setupToken.complete()
                }
            }
    }

    private func beginUpstreamTLSConnection(
        target: ConnectionTarget,
        egressPolicy: EgressPolicy,
        setupToken: ProxyConnectionSetupToken,
        on loop: EventLoop
    ) -> EventLoopFuture<Channel> {
        let resolver = EgressFilteringResolver(
            resolver: NIORandomizedDNSResolver(loop: loop),
            policy: egressPolicy
        )
        setupToken.onCancellation(on: loop) {
            resolver.cancelQueries()
        }
        let reservation: ResourceReservation?
        do {
            reservation = try admission?.acquire(.connection(destination: target.logicalAuthority.lowercased()))
        } catch {
            return loop.makeFailedFuture(error)
        }
        let connection = ClientBootstrap(group: loop)
            .resolver(resolver)
            .channelInitializer { [self] channel in
                channel.closeFuture.whenComplete { _ in reservation?.release() }
                guard connectionRegistry.register(channel) else {
                    return channel.closeFuture.flatMapThrowing { throw ProxyConnectionError.serverStopping }
                }
                setupToken.onCancellation(on: channel.eventLoop) {
                    channel.close(promise: nil)
                }
                return channel.eventLoop.makeSucceededFuture(())
            }
            .connect(host: target.connectionHost, port: target.port)
        connection.whenFailure { _ in reservation?.release() }
        return connection
    }

    private func negotiateUpstreamTLS(
        on channel: Channel,
        context: NIOSSLContext,
        target: ConnectionTarget,
        egressPolicy: EgressPolicy
    ) -> EventLoopFuture<ResolvedUpstreamConnection> {
        guard let remote = channel.remoteAddress else {
            return channel.close().flatMapThrowing {
                throw ProxyConnectionError.egressBlocked(target.connectionHost)
            }
        }
        let resolvedTarget = ResolvedTarget(unresolved: target, connectedAddress: remote)
        guard !egressPolicy.denies(resolvedTarget.connectedAddress) else {
            return channel.close().flatMapThrowing {
                throw ProxyConnectionError.egressBlocked(target.connectionHost)
            }
        }
        let negotiatedProtocol = channel.eventLoop.makePromise(of: ALPNProtocol.self)
        let alpnHandler = UpstreamALPNHandler(negotiatedProtocol: negotiatedProtocol)
        let loopBoundALPNHandler = NIOLoopBound(alpnHandler, eventLoop: channel.eventLoop)
        return channel.eventLoop.makeCompletedFuture {
            try channel.pipeline.syncOperations.addHandler(
                NIOSSLClientHandler(context: context, serverHostname: target.tlsServerName)
            )
            try channel.pipeline.syncOperations.addHandler(alpnHandler)
        }
        .flatMap { negotiatedProtocol.futureResult }
        .map {
            ResolvedUpstreamConnection(
                connection: NegotiatedUpstreamConnection(
                    channel: channel,
                    applicationProtocol: $0,
                    alpnHandler: loopBoundALPNHandler
                ),
                target: resolvedTarget
            )
        }
        .flatMapError { error in
            channel.close().flatMapThrowing { throw error }
        }
    }
}

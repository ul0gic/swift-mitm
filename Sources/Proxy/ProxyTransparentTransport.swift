import Foundation
import NIOCore
import NIOPosix
import NIOSSL

struct ResolvedRawUpstreamConnection: Sendable {
    let channel: Channel
    let target: ResolvedTarget
}

extension ProxyServer {
    func configureInbound(_ channel: Channel, tlsRuntime: ProxyTLSRuntime) -> EventLoopFuture<Void> {
        let reservation: ResourceReservation?
        do { reservation = try admission?.acquire(.connection(destination: nil)) }
        catch { return channel.close() }
        channel.closeFuture.whenComplete { _ in reservation?.release() }
        guard connectionRegistry.register(channel) else {
            return channel.closeFuture
        }
        switch ingress {
        case .priorKnowledgeHTTP2(let configuration):
            return configurePriorKnowledgeHTTP2(channel, configuration: configuration)
        case .explicitConnect:
            return configureExplicitInbound(channel, tlsRuntime: tlsRuntime)
        case .explicitHTTP:
            return configureExplicitInbound(channel, tlsRuntime: tlsRuntime)
        case .trustedProxyV2(let configuration):
            return configureTransparentInbound(channel, configuration: configuration, tlsRuntime: tlsRuntime)
        }
    }

    func configureTransparentInbound(
        _ channel: Channel,
        configuration: TrustedProxyV2Ingress,
        tlsRuntime: ProxyTLSRuntime
    ) -> EventLoopFuture<Void> {
        channel.eventLoop.makeCompletedFuture {
            let stageObserver = self.sink as? any TransparentIngressStageObserver
            let failureHandler = NIOLoopBound(
                TransparentConnectionFailureHandler(sink: self.sink),
                eventLoop: channel.eventLoop
            )
            let ingressHandler = TrustedProxyV2IngressHandler(
                configuration: configuration,
                stageObserver: stageObserver
            ) { [self] context, accepted in
                failureHandler.value.updateTarget(accepted.target.capturedTarget())
                failureHandler.value.updateStage(.routing)
                let passThrough = interceptionPolicy.allowsPassThrough(
                    host: accepted.target.connectionHost,
                    port: accepted.target.port
                )
                let classifier = TransparentApplicationClassifier(
                    configuration: configuration,
                    stageObserver: stageObserver,
                    passThrough: passThrough
                ) { [self] _, classification in
                    handleTransparentClassification(
                        classification,
                        channel: context.channel,
                        target: accepted.target,
                        failureHandler: failureHandler,
                        opaqueReason: passThrough ? .policySelected : .classificationFallback,
                        tlsRuntime: tlsRuntime
                    )
                }
                try context.pipeline.syncOperations.addHandler(
                    classifier,
                    position: .before(failureHandler.value)
                )
            }
            try channel.pipeline.syncOperations.addHandler(failureHandler.value)
            try channel.pipeline.syncOperations.addHandler(
                ingressHandler,
                position: .before(failureHandler.value)
            )
        }
    }

    private func handleTransparentClassification(
        _ classification: TransparentApplicationClassification,
        channel: Channel,
        target: ConnectionTarget,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>,
        opaqueReason: OpaqueForwardingReason,
        tlsRuntime: ProxyTLSRuntime
    ) -> EventLoopFuture<TransparentApplicationReadMode> {
        guard let setupToken = connectionRegistry.beginSetup() else {
            return channel.eventLoop.makeFailedFuture(ProxyConnectionError.serverStopping)
        }
        setupToken.onCancellation(on: channel.eventLoop) {
            failureHandler.value.cancel()
            channel.close(promise: nil)
        }
        channel.closeFuture.whenComplete { _ in setupToken.cancel() }
        let setup: EventLoopFuture<TransparentApplicationReadMode>
        switch classification {
        case .interceptedTLS(let metadata):
            failureHandler.value.beginTLSSetup(
                deadline: timeoutPolicy.tlsHandshakeDeadline.nioTimeAmount ?? .seconds(10),
                setupToken: setupToken
            )
            setup = connectInspectedTLS(
                metadata: metadata,
                target: target,
                tlsRuntime: tlsRuntime,
                setupToken: setupToken,
                failureHandler: failureHandler,
                on: channel.eventLoop
            )
            .flatMap { [self] upstream in
                configureTransparentDownstreamTLS(
                    channel: channel,
                    upstream: upstream,
                    failureHandler: failureHandler,
                    tlsRuntime: tlsRuntime
                )
            }
            .map { .automatic }
        case .clearHTTP1:
            failureHandler.value.updateStage(.connect)
            setup = connectUpstreamRaw(target: target, on: channel.eventLoop, setupToken: setupToken)
                .flatMap { [self] upstream in
                    bridgeClearHTTP1(
                        inbound: channel,
                        upstream: upstream,
                        failureHandler: failureHandler
                    )
                }
                .map { .automatic }
        case .opaque:
            failureHandler.value.updateStage(.connect)
            setup = connectUpstreamRaw(
                target: target,
                on: channel.eventLoop,
                setupToken: setupToken,
                allowHalfClosure: true
            )
                .flatMap { [self] upstream in
                    bridgeOpaque(
                        inbound: channel,
                        upstream: upstream,
                        failureHandler: failureHandler,
                        reason: opaqueReason
                    )
                }
                .map { .manual }
        }
        return setup.always { result in
            if case .success = result, case .interceptedTLS = classification {
                failureHandler.value.handoffFuture(on: channel.eventLoop)
                    .whenComplete { _ in setupToken.complete() }
            } else {
                setupToken.complete()
            }
        }
    }

    private func configureTransparentDownstreamTLS(
        channel: Channel,
        upstream: ResolvedUpstreamConnection,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>,
        tlsRuntime: ProxyTLSRuntime
    ) -> EventLoopFuture<Void> {
        upstream.connection.channel.closeFuture.whenComplete { _ in channel.close(promise: nil) }
        channel.closeFuture.whenComplete { _ in upstream.connection.channel.close(promise: nil) }
        return tlsRuntime
            .makeServerContext(
                defaultHost: upstream.target.unresolved.leafIdentity,
                applicationProtocols: [upstream.connection.applicationProtocol],
                on: channel.eventLoop
            )
            .flatMapThrowing { [self] context in
                guard channel.isActive else { throw ProxyConnectionError.serverStopping }
                failureHandler.value.updateStage(.downstreamTLS)
                let sync = channel.pipeline.syncOperations
                try sync.addHandler(
                    NIOSSLServerHandler(context: context),
                    position: .before(failureHandler.value)
                )
                try sync.addHandler(
                    makeALPNHandler(upstream: upstream, failureHandler: failureHandler),
                    position: .before(failureHandler.value)
                )
            }
    }

    func removeTransparentFailureHandler(
        _ handler: NIOLoopBound<TransparentConnectionFailureHandler>,
        from channel: Channel
    ) -> EventLoopFuture<Void> {
        channel.eventLoop.makeCompletedFuture {
            handler.value.complete()
            channel.pipeline.syncOperations.removeHandler(handler.value, promise: nil)
        }
    }

    private func bridgeClearHTTP1(
        inbound: Channel,
        upstream: ResolvedRawUpstreamConnection,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>
    ) -> EventLoopFuture<Void> {
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let correlator = HTTP1ExchangeCorrelator(policy: correlationPolicy)
        let webSocketSession = WebSocketCaptureSession()
        let pair = NIOLoopBound(GlueHandler.matchedPair(), eventLoop: inbound.eventLoop)
        let target = upstream.target.capturedTarget
        let connectionID = failureHandler.value.id
        let reportResourceLimit: @Sendable () -> Void = {
            sink.receive(.connectionFailure(CapturedConnectionFailure(
                id: connectionID,
                timestamp: Date(),
                reason: .transportFailure,
                target: target,
                stage: .capture,
                code: .resourceLimit
            )))
        }
        upstream.channel.closeFuture.whenComplete { _ in inbound.close(promise: nil) }
        inbound.closeFuture.whenComplete { _ in upstream.channel.close(promise: nil) }
        return inbound.eventLoop.makeCompletedFuture {
            try upstream.channel.pipeline.syncOperations.addHandlers([
                HTTP1CaptureTapHandler(
                    direction: .response,
                    authority: target.logicalAuthority,
                    scheme: "http",
                    target: target,
                    correlator: correlator,
                    sink: sink,
                    captureBodyLimit: captureBodyLimit,
                    webSocketSession: webSocketSession,
                    onResourceLimit: reportResourceLimit
                ),
                pair.value.1
            ])
            try inbound.pipeline.syncOperations.addHandlers([
                HTTP1CaptureTapHandler(
                    direction: .request,
                    authority: target.logicalAuthority,
                    scheme: "http",
                    target: target,
                    correlator: correlator,
                    sink: sink,
                    captureBodyLimit: captureBodyLimit,
                    webSocketSession: webSocketSession,
                    onResourceLimit: reportResourceLimit
                ),
                pair.value.0
            ])
        }
        .flatMap { [self] in removeTransparentFailureHandler(failureHandler, from: inbound) }
        .flatMap { upstream.channel.setOption(ChannelOptions.autoRead, value: true) }
    }

    func bridgeOpaque(
        inbound: Channel,
        upstream: ResolvedRawUpstreamConnection,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>,
        reason: OpaqueForwardingReason
    ) -> EventLoopFuture<Void> {
        let stageObserver = sink as? any TransparentIngressStageObserver
        let flow = CapturedOpaqueFlow(
            id: failureHandler.value.id,
            timestamp: Date(),
            target: upstream.target.capturedTarget,
            reason: reason
        )
        let pair = NIOLoopBound(
            OpaqueFlowBridgeHandler.matchedPair(
                flow: flow,
                sink: sink,
                captureByteLimit: opaqueCaptureByteLimit,
                eventLoop: inbound.eventLoop
            ),
            eventLoop: inbound.eventLoop
        )
        return inbound.eventLoop.makeCompletedFuture {
            try upstream.channel.pipeline.syncOperations.addHandler(pair.value.1)
            try inbound.pipeline.syncOperations.addHandler(pair.value.0)
        }
        .flatMap { [self] in removeTransparentFailureHandler(failureHandler, from: inbound) }
        .map {
            stageObserver?.didEnterTransparentIngressStage(.opaqueBridgeReady)
            upstream.channel.read()
        }
    }

    func connectUpstreamRaw(
        target: ConnectionTarget,
        on loop: EventLoop,
        setupToken suppliedToken: ProxyConnectionSetupToken? = nil,
        allowHalfClosure: Bool = false
    ) -> EventLoopFuture<ResolvedRawUpstreamConnection> {
        let reservation: ResourceReservation?
        do { reservation = try admission?.acquire(.connection(destination: target.logicalAuthority.lowercased())) }
        catch { return loop.makeFailedFuture(error) }
        guard let setupToken = suppliedToken ?? connectionRegistry.beginSetup() else {
            reservation?.release()
            return loop.makeFailedFuture(ProxyConnectionError.serverStopping)
        }
        if egressPolicy.deniesLiteral(target.connectionHost) {
            reservation?.release()
            if suppliedToken == nil { setupToken.complete() }
            return loop.makeFailedFuture(ProxyConnectionError.egressBlocked(target.connectionHost))
        }
        let resolver = EgressFilteringResolver(
            resolver: NIORandomizedDNSResolver(loop: loop),
            policy: egressPolicy
        )
        setupToken.onCancellation(on: loop) { resolver.cancelQueries() }
        let registry = connectionRegistry
        let connect = ClientBootstrap(group: loop)
            .resolver(resolver)
            .channelOption(ChannelOptions.autoRead, value: false)
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: allowHalfClosure)
            .channelInitializer { channel in
                channel.closeFuture.whenComplete { _ in reservation?.release() }
                setupToken.onCancellation(on: channel.eventLoop) { channel.close(promise: nil) }
                guard !setupToken.isCancelled, registry.register(channel) else {
                    return channel.close().flatMapThrowing { throw ProxyConnectionError.serverStopping }
                }
                return channel.eventLoop.makeSucceededVoidFuture()
            }
            .connect(host: target.connectionHost, port: target.port)
        connect.whenFailure { _ in reservation?.release() }
        return enforceSetupDeadline(
            connect,
            timeout: timeoutPolicy.upstreamConnectDeadline.nioTimeAmount ?? .seconds(10),
            setupToken: setupToken
        ) { $0.close() }
            .flatMap { [self] channel in
                guard !setupToken.isCancelled else {
                    return channel.close().flatMapThrowing { throw ProxyConnectionError.serverStopping }
                }
                guard let remote = channel.remoteAddress, !egressPolicy.denies(remote) else {
                    return channel.close().flatMapThrowing {
                        throw ProxyConnectionError.egressBlocked(target.connectionHost)
                    }
                }
                return channel.eventLoop.makeSucceededFuture(ResolvedRawUpstreamConnection(
                    channel: channel,
                    target: ResolvedTarget(unresolved: target, connectedAddress: remote)
                ))
            }
            .always { _ in
                if suppliedToken == nil { setupToken.complete() }
            }
    }
}

import Foundation
import NIOCore
import NIOHTTP1
import NIOPosix
import NIOSSL

private struct ResolvedRawUpstreamConnection: Sendable {
    let channel: Channel
    let target: ResolvedTarget
}

extension ProxyServer {
    func configureInbound(_ channel: Channel, tlsRuntime: ProxyTLSRuntime) -> EventLoopFuture<Void> {
        guard connectionRegistry.register(channel) else {
            return channel.closeFuture
        }
        switch ingress {
        case .explicitConnect:
            return configureExplicitInbound(channel, tlsRuntime: tlsRuntime)
        case .trustedProxyV2(let configuration):
            return configureTransparentInbound(channel, configuration: configuration, tlsRuntime: tlsRuntime)
        }
    }

    private func configureExplicitInbound(
        _ channel: Channel,
        tlsRuntime: ProxyTLSRuntime
    ) -> EventLoopFuture<Void> {
        channel.eventLoop.makeCompletedFuture {
            let sync = channel.pipeline.syncOperations
            try sync.addHandler(HTTPResponseEncoder(), name: Self.encoderName)
            try sync.addHandler(
                ByteToMessageHandler(HTTPRequestDecoder(leftOverBytesStrategy: .forwardBytes)),
                name: Self.decoderName
            )
            try sync.addHandler(
                ConnectHandler { [self] tunnel, authority in
                    onConnectEstablished(channel: tunnel, authority: authority, tlsRuntime: tlsRuntime)
                }
            )
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
                let classifier = TransparentApplicationClassifier(
                    configuration: configuration,
                    stageObserver: stageObserver
                ) { [self] _, classification in
                    handleTransparentClassification(
                        classification,
                        channel: context.channel,
                        target: accepted.target,
                        failureHandler: failureHandler,
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

    private func onConnectEstablished(
        channel: Channel,
        authority: String,
        tlsRuntime: ProxyTLSRuntime
    ) {
        guard let target = ConnectionTarget(explicitConnectAuthority: authority) else {
            channel.close(promise: nil)
            return
        }
        let pipeline = channel.pipeline
        let pendingReads = PendingTLSReads(eventLoop: channel.eventLoop)
        do {
            try pipeline.syncOperations.addHandler(pendingReads)
        } catch {
            channel.close(promise: nil)
            return
        }
        let loopBoundPendingReads = NIOLoopBound(pendingReads, eventLoop: channel.eventLoop)
        EventLoopFuture.andAllSucceed(
            [
                pipeline.removeHandler(name: Self.encoderName),
                pipeline.removeHandler(name: Self.decoderName)
            ],
            on: channel.eventLoop
        )
            .flatMap { loopBoundPendingReads.value.clientALPNOffer }
            .flatMapThrowing { try Self.applicationProtocols(for: $0) }
            .flatMap { [self] applicationProtocols in
                connectUpstreamTLS(
                    target: target,
                    applicationProtocols: applicationProtocols,
                    tlsRuntime: tlsRuntime,
                    on: channel.eventLoop
                )
            }
            .flatMap { [self] upstream in
                configureDownstreamTLS(
                    channel: channel,
                    upstream: upstream,
                    pendingReads: loopBoundPendingReads,
                    tlsRuntime: tlsRuntime
                )
            }
            .flatMap { channel.setOption(ChannelOptions.autoRead, value: true) }
            .whenFailure { _ in channel.close(promise: nil) }
    }

    private func handleTransparentClassification(
        _ classification: TransparentApplicationClassification,
        channel: Channel,
        target: ConnectionTarget,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>,
        tlsRuntime: ProxyTLSRuntime
    ) -> EventLoopFuture<TransparentApplicationReadMode> {
        switch classification {
        case .interceptedTLS(let metadata):
            let refinedTarget = target.applyingTLSMetadata(metadata)
            failureHandler.value.updateTarget(refinedTarget.capturedTarget())
            let applicationProtocols: [ALPNProtocol]
            do {
                applicationProtocols = try Self.applicationProtocols(for: metadata.compatibilityALPNOffer)
            } catch {
                return channel.eventLoop.makeFailedFuture(error)
            }
            return connectUpstreamTLS(
                target: refinedTarget,
                applicationProtocols: applicationProtocols,
                tlsRuntime: tlsRuntime,
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
            return connectUpstreamRaw(target: target, on: channel.eventLoop)
                .flatMap { [self] upstream in
                    bridgeClearHTTP1(
                        inbound: channel,
                        upstream: upstream,
                        failureHandler: failureHandler
                    )
                }
                .map { .automatic }
        case .opaque:
            return connectUpstreamRaw(target: target, on: channel.eventLoop)
                .flatMap { [self] upstream in
                    bridgeOpaque(
                        inbound: channel,
                        upstream: upstream,
                        failureHandler: failureHandler
                    )
                }
                .map { .manual }
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
        let correlator = HTTP1ExchangeCorrelator()
        let webSocketSession = WebSocketCaptureSession()
        let pair = NIOLoopBound(GlueHandler.matchedPair(), eventLoop: inbound.eventLoop)
        let target = upstream.target.capturedTarget
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
                    webSocketSession: webSocketSession
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
                    webSocketSession: webSocketSession
                ),
                pair.value.0
            ])
        }
        .flatMap { [self] in removeTransparentFailureHandler(failureHandler, from: inbound) }
        .flatMap { upstream.channel.setOption(ChannelOptions.autoRead, value: true) }
    }

    private func bridgeOpaque(
        inbound: Channel,
        upstream: ResolvedRawUpstreamConnection,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>
    ) -> EventLoopFuture<Void> {
        let flow = CapturedOpaqueFlow(id: UUID(), timestamp: Date(), target: upstream.target.capturedTarget)
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
        .map { upstream.channel.read() }
    }

    private func connectUpstreamRaw(
        target: ConnectionTarget,
        on loop: EventLoop
    ) -> EventLoopFuture<ResolvedRawUpstreamConnection> {
        guard let setupToken = connectionRegistry.beginSetup() else {
            return loop.makeFailedFuture(ProxyConnectionError.serverStopping)
        }
        guard let address = try? SocketAddress(ipAddress: target.connectionHost, port: target.port),
              !egressPolicy.denies(address) else {
            setupToken.complete()
            return loop.makeFailedFuture(ProxyConnectionError.egressBlocked(target.connectionHost))
        }
        let connect = ClientBootstrap(group: loop)
            .channelOption(ChannelOptions.autoRead, value: false)
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .connect(to: address)
        return enforceSetupDeadline(
            connect,
            timeout: timeoutPolicy.upstreamConnectDeadline.nioTimeAmount ?? .seconds(10),
            setupToken: setupToken
        ) { $0.close(promise: nil) }
            .flatMap { [self] channel in
                guard !setupToken.isCancelled, connectionRegistry.register(channel) else {
                    return channel.close().flatMapThrowing { throw ProxyConnectionError.serverStopping }
                }
                setupToken.onCancellation(on: channel.eventLoop) {
                    channel.close(promise: nil)
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
            .always { _ in setupToken.complete() }
    }
}

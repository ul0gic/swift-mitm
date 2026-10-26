import Foundation
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS
import os

private let alpnLog = Logger(subsystem: "SwiftMITM", category: "ProxyALPN")

struct ResolvedUpstreamConnection: Sendable {
    let connection: NegotiatedUpstreamConnection
    let target: ResolvedTarget
}

extension ProxyServer {
    static func applicationProtocols(for offer: ClientALPNOffer) throws -> [ALPNProtocol] {
        switch offer {
        case .absent:
            return [.http11]
        case .protocols(let protocols):
            guard !protocols.isEmpty else { throw ProxyALPNError.noSupportedClientProtocol }
            return protocols
        }
    }

    func configureDownstreamTLS(
        channel: Channel,
        upstream: ResolvedUpstreamConnection,
        pendingReads: NIOLoopBound<PendingTLSReads>,
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
                try sync.addHandler(NIOSSLServerHandler(context: context), position: .first)
                try sync.addHandler(makeALPNHandler(upstream: upstream))
                pendingReads.value.replay(on: channel.pipeline)
                _ = sync.removeHandler(pendingReads.value)
            }
    }

    func makeALPNHandler(
        upstream: ResolvedUpstreamConnection,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>? = nil
    ) -> ApplicationProtocolNegotiationHandler {
        ApplicationProtocolNegotiationHandler { [self] result, channel in
            let downstreamProtocol: ALPNProtocol
            switch result {
            case .negotiated(let proto):
                guard let negotiated = ALPNProtocol(rawValue: proto) else {
                    return channel.eventLoop.makeFailedFuture(ProxyALPNError.unsupportedProtocol(proto))
                }
                downstreamProtocol = negotiated
            case .fallback:
                downstreamProtocol = .http11
            }
            guard downstreamProtocol == upstream.connection.applicationProtocol else {
                return channel.eventLoop.makeFailedFuture(
                    ProxyALPNError.protocolMismatch(
                        downstream: downstreamProtocol.rawValue,
                        upstream: upstream.connection.applicationProtocol.rawValue
                    )
                )
            }
            let bridged = bridge(inbound: channel, upstream: upstream, protocol: downstreamProtocol)
            guard let failureHandler else { return bridged }
            return bridged.flatMap { [self] in
                removeTransparentFailureHandler(failureHandler, from: channel)
            }
        }
    }

    private func bridge(
        inbound: Channel,
        upstream: ResolvedUpstreamConnection,
        protocol negotiatedProtocol: ALPNProtocol
    ) -> EventLoopFuture<Void> {
        let authority = upstream.target.unresolved.logicalAuthority
        let target = upstream.target.capturedTarget
        alpnLog.debug("ALPN \(negotiatedProtocol.rawValue, privacy: .public) for \(authority, privacy: .public)")
        if negotiatedProtocol == .http2 {
            return bridgeHTTP2(
                inbound: inbound,
                upstream: upstream.connection,
                authority: authority,
                target: target
            )
        }
        return bridgeHTTP1(
            inbound: inbound,
            upstream: upstream.connection,
            authority: authority,
            scheme: "https",
            target: target
        )
    }

    private func bridgeHTTP2(
        inbound: Channel,
        upstream: NegotiatedUpstreamConnection,
        authority: String,
        target: CapturedTarget
    ) -> EventLoopFuture<Void> {
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let upstreamConnectionConfiguration = connectionConfiguration
        let streamConfiguration = streamConfiguration
        let settingsProbe = NIOLoopBound(
            HTTP2InitialSettingsProbeHandler(
                eventLoop: upstream.channel.eventLoop,
                deadline: timeoutPolicy.http2InitialSettingsDeadline.nioTimeAmount ?? .seconds(5)
            ),
            eventLoop: upstream.channel.eventLoop
        )
        return inbound.setOption(ChannelOptions.autoRead, value: false)
            .flatMap {
                upstream.channel.eventLoop.makeCompletedFuture {
                    try upstream.channel.pipeline.syncOperations.addHandler(settingsProbe.value)
                }
            }
            .flatMap {
                upstream.channel.configureHTTP2Pipeline(
                    mode: .client,
                    connectionConfiguration: upstreamConnectionConfiguration,
                    streamConfiguration: streamConfiguration
                ) { push in push.close() }
            }
            .flatMap { upstreamMux in
                upstream.releaseBufferedReads()
                    .flatMap { settingsProbe.value.capability }
                    .map { (upstreamMux, $0) }
            }
            .flatMap { upstreamMux, extendedConnectEnabled in
                inbound.configureHTTP2Pipeline(
                    mode: .server,
                    connectionConfiguration: self.downstreamConnectionConfiguration(
                        extendedConnectEnabled: extendedConnectEnabled
                    ),
                    streamConfiguration: streamConfiguration
                ) { inboundStream in
                    H2StreamGlue.glue(
                        inboundStream: inboundStream,
                        upstreamMux: upstreamMux,
                        authority: authority,
                        target: target,
                        sink: sink,
                        captureBodyLimit: captureBodyLimit,
                        extendedConnectEnabled: extendedConnectEnabled
                    )
                }
                .map { _ in () }
            }
            .flatMap { inbound.setOption(ChannelOptions.autoRead, value: true) }
    }

    private func bridgeHTTP1(
        inbound: Channel,
        upstream: NegotiatedUpstreamConnection,
        authority: String,
        scheme: String,
        target: CapturedTarget
    ) -> EventLoopFuture<Void> {
        let loop = inbound.eventLoop
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let correlator = HTTP1ExchangeCorrelator()
        let webSocketSession = WebSocketCaptureSession()
        let pair = NIOLoopBound(GlueHandler.matchedPair(), eventLoop: loop)
        return loop.makeCompletedFuture {
            try upstream.channel.pipeline.syncOperations.addHandlers([
                HTTP1CaptureTapHandler(
                    direction: .response,
                    authority: authority,
                    scheme: scheme,
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
                    authority: authority,
                    scheme: scheme,
                    target: target,
                    correlator: correlator,
                    sink: sink,
                    captureBodyLimit: captureBodyLimit,
                    webSocketSession: webSocketSession
                ),
                pair.value.0
            ])
        }
        .flatMap { upstream.releaseBufferedReads() }
    }

    func enforceSetupDeadline<Value: Sendable>(
        _ future: EventLoopFuture<Value>,
        timeout: TimeAmount,
        setupToken: ProxyConnectionSetupToken,
        cleanup: @escaping @Sendable (Value) -> Void
    ) -> EventLoopFuture<Value> {
        let completion = ProxySetupStageCompletion<Value>(eventLoop: future.eventLoop)
        let deadlineTask = future.eventLoop.scheduleTask(in: timeout) {
            if setupToken.cancel() {
                completion.complete(.failure(ProxyConnectionError.setupTimedOut))
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
                    cleanup(value)
                }
                completion.complete(.failure(ProxyConnectionError.serverStopping))
            } else {
                completion.complete(result)
            }
        }
        return completion.futureResult
    }

    func connectUpstreamTLS(
        target: ConnectionTarget,
        applicationProtocols: [ALPNProtocol],
        tlsRuntime: ProxyTLSRuntime,
        on loop: EventLoop
    ) -> EventLoopFuture<ResolvedUpstreamConnection> {
        guard let setupToken = connectionRegistry.beginSetup() else {
            return loop.makeFailedFuture(ProxyConnectionError.serverStopping)
        }
        let context: NIOSSLContext
        do {
            context = try tlsRuntime.upstreamContext(applicationProtocols: applicationProtocols)
        } catch {
            setupToken.complete()
            return loop.makeFailedFuture(error)
        }
        let egressPolicy = egressPolicy
        if egressPolicy.deniesLiteral(target.connectionHost) {
            setupToken.complete()
            return loop.makeFailedFuture(ProxyConnectionError.egressBlocked(target.connectionHost))
        }
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
        ) { $0.close(promise: nil) }
            .flatMap { [self] channel in
                guard !setupToken.isCancelled else {
                    return channel.close().flatMapThrowing { throw ProxyConnectionError.serverStopping }
                }
                setupToken.onCancellation(on: channel.eventLoop) {
                    channel.close(promise: nil)
                }
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
                ) { $0.connection.channel.close(promise: nil) }
            }
            .always { _ in setupToken.complete() }
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
        return ClientBootstrap(group: loop)
            .resolver(resolver)
            .connect(host: target.connectionHost, port: target.port)
    }

    private func negotiateUpstreamTLS(
        on channel: Channel,
        context: NIOSSLContext,
        target: ConnectionTarget,
        egressPolicy: EgressPolicy
    ) -> EventLoopFuture<ResolvedUpstreamConnection> {
        guard connectionRegistry.register(channel) else {
            return channel.closeFuture.flatMapThrowing { throw ProxyConnectionError.serverStopping }
        }
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

    func makeUpstreamConfiguration(alpn: String) throws -> TLSConfiguration {
        try ProxyTLSRuntime.makeUpstreamConfiguration(
            upstreamPolicy: upstreamPolicy,
            applicationProtocols: [alpn]
        )
    }

    private var streamConfiguration: NIOHTTP2Handler.StreamConfiguration {
        var configuration = NIOHTTP2Handler.StreamConfiguration()
        configuration.targetWindowSize = targetWindowSize
        return configuration
    }

    private var connectionConfiguration: NIOHTTP2Handler.ConnectionConfiguration {
        var configuration = NIOHTTP2Handler.ConnectionConfiguration()
        configuration.targetWindowSize = targetWindowSize
        return configuration
    }

    private func downstreamConnectionConfiguration(
        extendedConnectEnabled: Bool
    ) -> NIOHTTP2Handler.ConnectionConfiguration {
        var configuration = connectionConfiguration
        configuration.initialSettings.removeAll { $0.parameter == .enableConnectProtocol }
        if extendedConnectEnabled {
            configuration.initialSettings.append(HTTP2Setting(parameter: .enableConnectProtocol, value: 1))
        }
        return configuration
    }
}

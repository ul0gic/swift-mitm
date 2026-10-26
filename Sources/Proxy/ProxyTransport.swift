import Foundation
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOSSL
import NIOTLS
import os
import SwiftMITMHTTP1Codec

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
        tlsRuntime: ProxyTLSRuntime,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>? = nil
    ) -> EventLoopFuture<Void> {
        if controlRuntime == nil {
            upstream.connection.channel.closeFuture.whenComplete { _ in channel.close(promise: nil) }
        }
        channel.closeFuture.whenComplete { _ in upstream.connection.channel.close(promise: nil) }
        failureHandler?.value.updateStage(.downstreamTLS)
        return tlsRuntime
            .makeServerContext(
                defaultHost: upstream.target.unresolved.leafIdentity,
                applicationProtocols: [upstream.connection.applicationProtocol],
                on: channel.eventLoop
            )
            .flatMapThrowing { [self] context in
                guard channel.isActive else { throw ChannelError.ioOnClosedChannel }
                let sync = channel.pipeline.syncOperations
                try sync.addHandler(NIOSSLServerHandler(context: context), position: .first)
                if let failureHandler {
                    try sync.addHandler(
                        makeALPNHandler(upstream: upstream, failureHandler: failureHandler),
                        position: .before(failureHandler.value)
                    )
                } else {
                    try sync.addHandler(makeALPNHandler(upstream: upstream))
                }
                pendingReads.value.replay(on: channel.pipeline)
                _ = sync.removeHandler(pendingReads.value)
            }
    }

    func makeALPNHandler(
        upstream: ResolvedUpstreamConnection,
        failureHandler: NIOLoopBound<TransparentConnectionFailureHandler>? = nil
    ) -> ApplicationProtocolNegotiationHandler {
        ApplicationProtocolNegotiationHandler { [self] result, channel in
            failureHandler?.value.updateStage(.negotiation)
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
            let bridged = bridge(
                inbound: channel,
                upstream: upstream,
                protocol: downstreamProtocol,
                connectionID: failureHandler?.value.id ?? UUID()
            )
            guard let failureHandler else { return bridged }
            return bridged.flatMap { [self] in
                failureHandler.value.complete()
                return removeTransparentFailureHandler(failureHandler, from: channel)
            }
        }
    }

    private func bridge(
        inbound: Channel,
        upstream: ResolvedUpstreamConnection,
        protocol negotiatedProtocol: ALPNProtocol,
        connectionID: UUID
    ) -> EventLoopFuture<Void> {
        let authority = upstream.target.unresolved.logicalAuthority
        let target = upstream.target.capturedTarget
        alpnLog.debug("ALPN \(negotiatedProtocol.rawValue, privacy: .public)")
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
            target: target,
            connectionID: connectionID
        )
    }

    func bridgeHTTP2(
        inbound: Channel,
        upstream: NegotiatedUpstreamConnection,
        authority: String,
        target: CapturedTarget,
        validateAuthority: Bool = false
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
                self.configureHTTP2Downstream(
                    inbound: inbound, upstreamMux: upstreamMux, validateAuthority: validateAuthority,
                    configuration: .init(
                        authority: authority, target: target, sink: sink, captureBodyLimit: captureBodyLimit,
                        extendedConnectEnabled: extendedConnectEnabled, controlServer: self
                    )
                )
            }
            .flatMap { inbound.setOption(ChannelOptions.autoRead, value: true) }
    }

    private func configureHTTP2Downstream(
        inbound: Channel, upstreamMux: NIOHTTP2Handler.StreamMultiplexer,
        validateAuthority: Bool, configuration: H2StreamGlue.Configuration
    ) -> EventLoopFuture<Void> {
        let initialize: NIOChannelInitializer = { inboundStream in
            let validation = inboundStream.eventLoop.makeCompletedFuture {
                if validateAuthority {
                    try inboundStream.pipeline.syncOperations.addHandler(
                        HTTP2PriorKnowledgeAuthorityHandler(authority: configuration.authority)
                    )
                }
            }
            return validation.flatMap {
                H2StreamGlue.glue(
                    inboundStream: inboundStream, upstreamMux: upstreamMux, configuration: configuration
                )
            }
        }
        let connectionConfiguration = downstreamConnectionConfiguration(
            extendedConnectEnabled: configuration.extendedConnectEnabled
        )
        if controlRuntime != nil {
            return HTTP2ControlPipeline.configure(
                channel: inbound, mode: .server, configuration: connectionConfiguration,
                initializer: initialize
            ).map { _ in () }
        }
        return inbound.configureHTTP2Pipeline(
            mode: .server, connectionConfiguration: connectionConfiguration,
            streamConfiguration: streamConfiguration, inboundStreamInitializer: initialize
        ).map { _ in () }
    }

    private func bridgeHTTP1(
        inbound: Channel,
        upstream: NegotiatedUpstreamConnection,
        authority: String,
        scheme: String,
        target: CapturedTarget,
        connectionID: UUID
    ) -> EventLoopFuture<Void> {
        if let controlRuntime, !controlRuntime.configurationSnapshot.rules.isEmpty {
            return bridgeControlledHTTP1(inbound: inbound, upstream: upstream, authority: authority,
                                         target: target, connectionID: connectionID)
        }
        if controlRuntime != nil {
            upstream.channel.closeFuture.whenComplete { _ in inbound.close(promise: nil) }
        }
        let loop = inbound.eventLoop
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let correlator = HTTP1ExchangeCorrelator(policy: correlationPolicy, admission: admission)
        let webSocketSession = WebSocketCaptureSession()
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
                    webSocketSession: webSocketSession,
                    onResourceLimit: reportResourceLimit
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
                    webSocketSession: webSocketSession,
                    onResourceLimit: reportResourceLimit
                ),
                pair.value.0
            ])
        }
        .flatMap { upstream.releaseBufferedReads() }
    }

    private func bridgeControlledHTTP1(inbound: Channel, upstream: NegotiatedUpstreamConnection,
                                       authority: String, target: CapturedTarget, connectionID: UUID) -> EventLoopFuture<Void> {
        inbound.eventLoop.makeCompletedFuture {
            guard let destination = HTTPForwardTarget(scheme: "https", authority: authority, requestTarget: "/") else { throw HTTPControlError.invalidConfiguration }
            var configuration = BytePreservingHTTPResponseEncoder.Configuration()
            configuration.automaticallySetFramingHeaders = false
            let encoder = BytePreservingHTTPResponseEncoder(configuration: configuration)
            try inbound.pipeline.syncOperations.addHandler(encoder)
            try inbound.pipeline.syncOperations.addHandler(ByteToMessageHandler(BytePreservingHTTPRequestDecoder(leftOverBytesStrategy: .forwardBytes, responseEncoder: encoder)))
            var options = HTTPForwardOptions()
            options.correlationPolicy = correlationPolicy
            options.controlRuntime = controlRuntime
            options.controlEventSink = controlEventSink
            options.admission = admission
            options.fixedDestination = destination
            try inbound.pipeline.syncOperations.addHandler(ExplicitHTTPProxyHandler(
                sink: sink, captureBodyLimit: captureBodyLimit,
                connectionID: connectionID,
                connect: { [self] target, loop in connectControlledHTTP(target: target, on: loop) },
                onCONNECT: { channel, _ in channel.close(promise: nil) },
                options: options
            ))
            upstream.channel.close(promise: nil)
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

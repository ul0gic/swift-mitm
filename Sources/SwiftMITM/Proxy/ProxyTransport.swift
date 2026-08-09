import Foundation
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS
import os

private let alpnLog = Logger(subsystem: "SwiftMITM", category: "ProxyALPN")

extension ProxyServer {
    func configureInbound(_ channel: Channel, tlsRuntime: ProxyTLSRuntime) -> EventLoopFuture<Void> {
        guard connectionRegistry.register(channel) else {
            return channel.closeFuture
        }
        return channel.eventLoop.makeCompletedFuture {
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

    private func onConnectEstablished(
        channel: Channel,
        authority: String,
        tlsRuntime: ProxyTLSRuntime
    ) {
        guard let target = Self.splitAuthority(authority) else {
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
                    host: target.host,
                    port: target.port,
                    applicationProtocols: applicationProtocols,
                    tlsRuntime: tlsRuntime,
                    on: channel.eventLoop
                )
            }
            .flatMap { [self] upstream in
                configureDownstreamTLS(
                    channel: channel,
                    authority: authority,
                    host: target.host,
                    upstream: upstream,
                    pendingReads: loopBoundPendingReads,
                    tlsRuntime: tlsRuntime
                )
            }
            .flatMap { channel.setOption(ChannelOptions.autoRead, value: true) }
            .whenFailure { _ in channel.close(promise: nil) }
    }

    private static func applicationProtocols(for offer: ClientALPNOffer) throws -> [ALPNProtocol] {
        switch offer {
        case .absent:
            return [.http11]
        case .protocols(let protocols):
            guard !protocols.isEmpty else { throw ProxyALPNError.noSupportedClientProtocol }
            return protocols
        }
    }

    private func configureDownstreamTLS(
        channel: Channel,
        authority: String,
        host: String,
        upstream: NegotiatedUpstreamConnection,
        pendingReads: NIOLoopBound<PendingTLSReads>,
        tlsRuntime: ProxyTLSRuntime
    ) -> EventLoopFuture<Void> {
        upstream.channel.closeFuture.whenComplete { _ in channel.close(promise: nil) }
        channel.closeFuture.whenComplete { _ in upstream.channel.close(promise: nil) }
        return tlsRuntime
            .makeServerContext(
                defaultHost: host,
                applicationProtocols: [upstream.applicationProtocol],
                on: channel.eventLoop
            )
            .flatMapThrowing { [self] context in
                let sync = channel.pipeline.syncOperations
                try sync.addHandler(NIOSSLServerHandler(context: context), position: .first)
                try sync.addHandler(makeALPNHandler(authority: authority, upstream: upstream))
                pendingReads.value.replay(on: channel.pipeline)
                _ = sync.removeHandler(pendingReads.value)
            }
    }

    private func makeALPNHandler(
        authority: String,
        upstream: NegotiatedUpstreamConnection
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
            guard downstreamProtocol == upstream.applicationProtocol else {
                return channel.eventLoop.makeFailedFuture(
                    ProxyALPNError.protocolMismatch(
                        downstream: downstreamProtocol.rawValue,
                        upstream: upstream.applicationProtocol.rawValue
                    )
                )
            }
            return bridge(inbound: channel, upstream: upstream, authority: authority, protocol: downstreamProtocol)
        }
    }

    private func bridge(
        inbound: Channel,
        upstream: NegotiatedUpstreamConnection,
        authority: String,
        protocol negotiatedProtocol: ALPNProtocol
    ) -> EventLoopFuture<Void> {
        guard let target = Self.splitAuthority(authority) else {
            return inbound.eventLoop.makeFailedFuture(ProxyConnectionError.invalidAuthority(authority))
        }
        alpnLog.debug("ALPN \(negotiatedProtocol.rawValue, privacy: .public) for \(authority, privacy: .public)")
        if negotiatedProtocol == .http2 {
            return bridgeHTTP2(inbound: inbound, upstream: upstream, host: target.host, port: target.port)
        }
        return bridgeHTTP1(inbound: inbound, upstream: upstream, host: target.host, port: target.port)
    }

    private func bridgeHTTP2(
        inbound: Channel,
        upstream: NegotiatedUpstreamConnection,
        host: String,
        port: Int
    ) -> EventLoopFuture<Void> {
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let connectionConfiguration = connectionConfiguration
        let streamConfiguration = streamConfiguration
        return upstream.channel.configureHTTP2Pipeline(
            mode: .client,
            connectionConfiguration: connectionConfiguration,
            streamConfiguration: streamConfiguration
        ) { push in push.close() }
        .flatMap { upstreamMux in
            inbound.configureHTTP2Pipeline(
                mode: .server,
                connectionConfiguration: connectionConfiguration,
                streamConfiguration: streamConfiguration
            ) { inboundStream in
                H2StreamGlue.glue(
                    inboundStream: inboundStream,
                    upstreamMux: upstreamMux,
                    authority: "\(host):\(port)",
                    sink: sink,
                    captureBodyLimit: captureBodyLimit
                )
            }
            .map { _ in () }
        }
        .flatMap { upstream.releaseBufferedReads() }
    }

    private func bridgeHTTP1(
        inbound: Channel,
        upstream: NegotiatedUpstreamConnection,
        host: String,
        port: Int
    ) -> EventLoopFuture<Void> {
        let loop = inbound.eventLoop
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let authority = "\(host):\(port)"
        let correlator = HTTP1ExchangeCorrelator()
        let closeEmissionState = WebSocketCloseEmissionState()
        let pair = NIOLoopBound(GlueHandler.matchedPair(), eventLoop: loop)
        return loop.makeCompletedFuture {
            try upstream.channel.pipeline.syncOperations.addHandlers([
                HTTP1CaptureTapHandler(
                    direction: .response,
                    authority: authority,
                    correlator: correlator,
                    sink: sink,
                    captureBodyLimit: captureBodyLimit,
                    closeEmissionState: closeEmissionState
                ),
                pair.value.1
            ])
            try inbound.pipeline.syncOperations.addHandlers([
                HTTP1CaptureTapHandler(
                    direction: .request,
                    authority: authority,
                    correlator: correlator,
                    sink: sink,
                    captureBodyLimit: captureBodyLimit,
                    closeEmissionState: closeEmissionState
                ),
                pair.value.0
            ])
        }
        .flatMap { upstream.releaseBufferedReads() }
    }

    private func connectUpstreamTLS(
        host: String,
        port: Int,
        applicationProtocols: [ALPNProtocol],
        tlsRuntime: ProxyTLSRuntime,
        on loop: EventLoop
    ) -> EventLoopFuture<NegotiatedUpstreamConnection> {
        guard connectionRegistry.beginConnection() else {
            return loop.makeFailedFuture(ProxyConnectionError.serverStopping)
        }
        let context: NIOSSLContext
        do {
            context = try tlsRuntime.upstreamContext(applicationProtocols: applicationProtocols)
        } catch {
            connectionRegistry.completeConnection()
            return loop.makeFailedFuture(error)
        }
        let egressPolicy = egressPolicy
        if egressPolicy.deniesLiteral(host) {
            connectionRegistry.completeConnection()
            return loop.makeFailedFuture(ProxyConnectionError.egressBlocked(host))
        }
        let serverHostname = Self.isIPAddress(host) ? nil : host
        let resolver = EgressFilteringResolver(
            resolver: NIORandomizedDNSResolver(loop: loop),
            policy: egressPolicy
        )
        return ClientBootstrap(group: loop)
            .resolver(resolver)
            .connect(host: host, port: port)
            .flatMap { [self] channel in
                negotiateUpstreamTLS(
                    on: channel,
                    context: context,
                    serverHostname: serverHostname,
                    host: host,
                    egressPolicy: egressPolicy
                )
            }
            .always { [connectionRegistry] _ in
                connectionRegistry.completeConnection()
            }
    }

    private func negotiateUpstreamTLS(
        on channel: Channel,
        context: NIOSSLContext,
        serverHostname: String?,
        host: String,
        egressPolicy: EgressPolicy
    ) -> EventLoopFuture<NegotiatedUpstreamConnection> {
        guard connectionRegistry.register(channel) else {
            return channel.closeFuture.flatMapThrowing { throw ProxyConnectionError.serverStopping }
        }
        guard let remote = channel.remoteAddress, !egressPolicy.denies(remote) else {
            return channel.close().flatMapThrowing { throw ProxyConnectionError.egressBlocked(host) }
        }
        let negotiatedProtocol = channel.eventLoop.makePromise(of: ALPNProtocol.self)
        let alpnHandler = UpstreamALPNHandler(negotiatedProtocol: negotiatedProtocol)
        let loopBoundALPNHandler = NIOLoopBound(alpnHandler, eventLoop: channel.eventLoop)
        return channel.eventLoop.makeCompletedFuture {
            try channel.pipeline.syncOperations.addHandler(
                NIOSSLClientHandler(context: context, serverHostname: serverHostname)
            )
            try channel.pipeline.syncOperations.addHandler(alpnHandler)
        }
        .flatMap { negotiatedProtocol.futureResult }
        .map {
            NegotiatedUpstreamConnection(
                channel: channel,
                applicationProtocol: $0,
                alpnHandler: loopBoundALPNHandler
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

    private static func isIPAddress(_ host: String) -> Bool {
        (try? SocketAddress(ipAddress: host, port: 0)) != nil
    }
}

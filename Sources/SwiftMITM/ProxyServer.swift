import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS
import os

private let alpnLog = Logger(subsystem: "corelift.api-ghost", category: "ProxyALPN")

/// The app-facing MITM proxy: listen → CONNECT → per-SNI TLS terminate → ALPN branch (h2/http1.1)
/// → re-originate upstream → tap → emit `CaptureEvent`s. The engine knows nothing of storage or UI.
public final class ProxyServer: Sendable {
    public struct UpstreamPolicy: Sendable {
        public var verifyCertificate: Bool
        public var additionalTrustRootsPEM: [String]

        public init(verifyCertificate: Bool = true, additionalTrustRootsPEM: [String] = []) {
            self.verifyCertificate = verifyCertificate
            self.additionalTrustRootsPEM = additionalTrustRootsPEM
        }

        public static let `default` = UpstreamPolicy()
    }

    private static let encoderName = "swiftmitm.connect.encoder"
    private static let decoderName = "swiftmitm.connect.decoder"

    private let group: EventLoopGroup
    private let ownsGroup: Bool
    private let authority: CertificateAuthority
    private let tls: TLSTermination
    private let sink: CaptureEventSink
    private let upstreamPolicy: UpstreamPolicy
    private let egressPolicy: EgressPolicy
    private let allowNonLoopbackBind: Bool
    private let targetWindowSize: Int
    private let captureBodyLimit: Int
    private let serverChannel: NIOLockedValueBox<Channel?>

    /// `captureBodyLimit` caps captured body bytes per request and per response; 0 disables body capture.
    public init(
        certificateAuthority: CertificateAuthority,
        sink: CaptureEventSink,
        group: EventLoopGroup? = nil,
        upstreamPolicy: UpstreamPolicy = .default,
        egressPolicy: EgressPolicy = .default,
        allowNonLoopbackBind: Bool = false,
        targetWindowSize: Int = 65535,
        captureBodyLimit: Int = 0
    ) {
        self.authority = certificateAuthority
        self.tls = TLSTermination(authority: certificateAuthority)
        self.sink = sink
        self.upstreamPolicy = upstreamPolicy
        self.egressPolicy = egressPolicy
        self.allowNonLoopbackBind = allowNonLoopbackBind
        self.targetWindowSize = targetWindowSize
        self.captureBodyLimit = captureBodyLimit
        if let group {
            self.group = group
            self.ownsGroup = false
        } else {
            self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
            self.ownsGroup = true
        }
        self.serverChannel = NIOLockedValueBox(nil)
    }

    /// Binds the listener and returns the bound port. Pass `port: 0` for an ephemeral port.
    /// Non-loopback binds expose the proxy as an open relay and require `allowNonLoopbackBind`.
    @discardableResult
    public func start(host: String = "127.0.0.1", port: Int) async throws -> Int {
        guard allowNonLoopbackBind || Self.isLoopbackHost(host) else {
            throw ProxyServerError.nonLoopbackBindRejected(host)
        }
        let channel = try await ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { [self] clientChannel in
                configureInbound(clientChannel)
            }
            .bind(host: host, port: port)
            .get()
        serverChannel.withLockedValue { $0 = channel }
        return channel.localAddress?.port ?? port
    }

    public func stop() async throws {
        let channel = serverChannel.withLockedValue { current -> Channel? in
            defer { current = nil }
            let captured = current
            return captured
        }
        if let channel {
            try await channel.close().get()
        }
        if ownsGroup {
            try await group.shutdownGracefully()
        }
    }

    private func configureInbound(_ channel: Channel) -> EventLoopFuture<Void> {
        channel.eventLoop.makeCompletedFuture {
            let sync = channel.pipeline.syncOperations
            try sync.addHandler(HTTPResponseEncoder(), name: Self.encoderName)
            try sync.addHandler(
                ByteToMessageHandler(HTTPRequestDecoder(leftOverBytesStrategy: .forwardBytes)),
                name: Self.decoderName
            )
            try sync.addHandler(
                ConnectHandler { [self] tunnel, authority in
                    onConnectEstablished(channel: tunnel, authority: authority)
                }
            )
        }
    }

    private func onConnectEstablished(channel: Channel, authority: String) {
        let pipeline = channel.pipeline
        let removals = [
            pipeline.removeHandler(name: Self.encoderName),
            pipeline.removeHandler(name: Self.decoderName)
        ]
        EventLoopFuture.andAllSucceed(removals, on: channel.eventLoop)
            .flatMapThrowing { [self] in
                let sslHandler = NIOSSLServerHandler(context: try tls.makeServerContext())
                let alpnHandler = makeALPNHandler(authority: authority)
                let sync = pipeline.syncOperations
                try sync.addHandler(sslHandler, position: .first)
                try sync.addHandler(alpnHandler)
            }
            .whenFailure { _ in channel.close(promise: nil) }
    }

    private func makeALPNHandler(authority: String) -> ApplicationProtocolNegotiationHandler {
        ApplicationProtocolNegotiationHandler { [self] result, channel in
            let negotiated: String
            switch result {
            case .negotiated(let proto):
                negotiated = proto
            case .fallback:
                negotiated = ALPNProtocol.http11.rawValue
            }
            return bridge(inbound: channel, authority: authority, alpn: negotiated)
        }
    }

    private func bridge(inbound: Channel, authority: String, alpn: String) -> EventLoopFuture<Void> {
        guard let target = Self.splitAuthority(authority) else {
            return inbound.eventLoop.makeFailedFuture(ProxyServerError.invalidAuthority(authority))
        }
        // WS-over-h2 (RFC 8441) is not decoded; wss normally negotiates http/1.1, which the h1 tap frame-decodes.
        let negotiated = alpn.isEmpty ? "http/1.1 (fallback)" : alpn
        alpnLog.debug("ALPN \(negotiated, privacy: .public) for \(authority, privacy: .public)")
        if alpn == ALPNProtocol.http2.rawValue {
            return bridgeHTTP2(inbound: inbound, host: target.host, port: target.port)
        }
        return bridgeHTTP1(inbound: inbound, host: target.host, port: target.port)
    }

    private func bridgeHTTP2(inbound: Channel, host: String, port: Int) -> EventLoopFuture<Void> {
        let loop = inbound.eventLoop
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let connectionConfiguration = connectionConfiguration
        let streamConfiguration = streamConfiguration
        return connectUpstreamTLS(host: host, port: port, alpn: ALPNProtocol.http2.rawValue, on: loop)
            .flatMap { upstream in
                upstream.configureHTTP2Pipeline(
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
            }
    }

    private func bridgeHTTP1(inbound: Channel, host: String, port: Int) -> EventLoopFuture<Void> {
        let loop = inbound.eventLoop
        let sink = sink
        let captureBodyLimit = captureBodyLimit
        let authority = "\(host):\(port)"
        return connectUpstreamTLS(host: host, port: port, alpn: ALPNProtocol.http11.rawValue, on: loop)
            .flatMap { upstream in
                let correlator = HTTP1ExchangeCorrelator()
                let pair = NIOLoopBound(GlueHandler.matchedPair(), eventLoop: loop)
                return loop.makeCompletedFuture {
                    try upstream.pipeline.syncOperations.addHandlers([
                        HTTP1CaptureTapHandler(
                            direction: .response,
                            authority: authority,
                            correlator: correlator,
                            sink: sink,
                            captureBodyLimit: captureBodyLimit
                        ),
                        pair.value.1
                    ])
                    try inbound.pipeline.syncOperations.addHandlers([
                        HTTP1CaptureTapHandler(
                            direction: .request,
                            authority: authority,
                            correlator: correlator,
                            sink: sink,
                            captureBodyLimit: captureBodyLimit
                        ),
                        pair.value.0
                    ])
                }
            }
    }

    private func connectUpstreamTLS(
        host: String,
        port: Int,
        alpn: String,
        on loop: EventLoop
    ) -> EventLoopFuture<Channel> {
        let context: NIOSSLContext
        do {
            context = try makeUpstreamContext(alpn: alpn)
        } catch {
            return loop.makeFailedFuture(error)
        }
        let egressPolicy = egressPolicy
        if egressPolicy.deniesLiteral(host) {
            return loop.makeFailedFuture(ProxyServerError.egressBlocked(host))
        }
        let serverHostname = Self.isIPAddress(host) ? nil : host
        return ClientBootstrap(group: loop)
            .connect(host: host, port: port)
            .flatMap { channel -> EventLoopFuture<Channel> in
                guard let remote = channel.remoteAddress, !egressPolicy.denies(remote) else {
                    return channel.close().flatMapThrowing { throw ProxyServerError.egressBlocked(host) }
                }
                return channel.eventLoop.makeCompletedFuture {
                    let handler = try NIOSSLClientHandler(context: context, serverHostname: serverHostname)
                    try channel.pipeline.syncOperations.addHandler(handler)
                    return channel
                }
            }
    }

    private func makeUpstreamContext(alpn: String) throws -> NIOSSLContext {
        var configuration = TLSConfiguration.makeClientConfiguration()
        configuration.applicationProtocols = [alpn]
        if !upstreamPolicy.verifyCertificate {
            configuration.certificateVerification = .none
        }
        if !upstreamPolicy.additionalTrustRootsPEM.isEmpty {
            let certificates = try upstreamPolicy.additionalTrustRootsPEM.flatMap {
                try NIOSSLCertificate.fromPEMBytes(Array($0.utf8))
            }
            configuration.trustRoots = .certificates(certificates)
        }
        return try NIOSSLContext(configuration: configuration)
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

    static func splitAuthority(_ authority: String) -> (host: String, port: Int)? {
        guard let separator = authority.lastIndex(of: ":") else { return nil }
        var host = String(authority[authority.startIndex..<separator])
        let portText = authority[authority.index(after: separator)...]
        guard let port = Int(portText), (1...65535).contains(port), !host.isEmpty else { return nil }
        if host.hasPrefix("["), host.hasSuffix("]") {
            host = String(host.dropFirst().dropLast())
        }
        return (host, port)
    }

    private static func isIPAddress(_ host: String) -> Bool {
        (try? SocketAddress(ipAddress: host, port: 0)) != nil
    }

    private static func isLoopbackHost(_ host: String) -> Bool {
        if host == "localhost" { return true }
        guard let address = try? SocketAddress(ipAddress: host, port: 0) else { return false }
        return EgressPolicy.isLoopback(address)
    }
}

/// Lifecycle seam so a controller can substitute a non-binding fake in headless tests.
public protocol ProxyServing: Sendable {
    @discardableResult
    func start(host: String, port: Int) async throws -> Int
    func stop() async throws
}

extension ProxyServer: ProxyServing {}

public enum ProxyServerError: Error, Equatable, Sendable {
    case invalidAuthority(String)
    case egressBlocked(String)
    case nonLoopbackBindRejected(String)
}

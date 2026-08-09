import NIOCore
import NIOPosix
import NIOSSL

final class ProxyTLSRuntime: Sendable {
    static let maximumCachedLeafIdentities = 256
    static let maximumPendingLeafMints = 256

    private static let supportedUpstreamOffers: [[ALPNProtocol]] = [
        [.http2, .http11],
        [.http11, .http2],
        [.http2],
        [.http11]
    ]

    let leafIdentities: LeafIdentityCache

    private let threadPool: NIOThreadPool
    private let upstreamContexts: [[ALPNProtocol]: NIOSSLContext]

    private init(
        threadPool: NIOThreadPool,
        leafIdentities: LeafIdentityCache,
        upstreamContexts: [[ALPNProtocol]: NIOSSLContext]
    ) {
        self.threadPool = threadPool
        self.leafIdentities = leafIdentities
        self.upstreamContexts = upstreamContexts
    }

    static func make(
        authority: CertificateAuthority,
        upstreamPolicy: ProxyServer.UpstreamPolicy
    ) async throws -> ProxyTLSRuntime {
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()
        do {
            let upstreamContexts = try await threadPool.runIfActive {
                try Dictionary(uniqueKeysWithValues: supportedUpstreamOffers.map { offer in
                    let configuration = try makeUpstreamConfiguration(
                        upstreamPolicy: upstreamPolicy,
                        applicationProtocols: offer.map(\.rawValue)
                    )
                    return (offer, try NIOSSLContext(configuration: configuration))
                })
            }
            let identities = LeafIdentityCache(
                authority: authority,
                threadPool: threadPool,
                maximumEntries: maximumCachedLeafIdentities,
                maximumPendingMints: maximumPendingLeafMints
            )
            return ProxyTLSRuntime(
                threadPool: threadPool,
                leafIdentities: identities,
                upstreamContexts: upstreamContexts
            )
        } catch {
            try? await threadPool.shutdownGracefully()
            throw error
        }
    }

    func upstreamContext(applicationProtocols: [ALPNProtocol]) throws -> NIOSSLContext {
        guard let context = upstreamContexts[applicationProtocols] else {
            throw ProxyTLSRuntimeError.unsupportedUpstreamProtocolOffer
        }
        return context
    }

    func makeServerContext(
        defaultHost: String,
        applicationProtocols: [ALPNProtocol],
        on eventLoop: EventLoop
    ) -> EventLoopFuture<NIOSSLContext> {
        leafIdentities.identity(forHost: defaultHost, on: eventLoop).flatMap { [self] identity in
            threadPool.runIfActive(eventLoop: eventLoop) {
                let termination = TLSTermination(identities: self.leafIdentities, defaultHost: defaultHost)
                let configuration = termination.makeServerConfiguration(
                    baseIdentity: identity,
                    applicationProtocols: applicationProtocols
                )
                return try NIOSSLContext(configuration: configuration)
            }
        }
    }

    func shutdownGracefully() async -> Error? {
        do {
            try await threadPool.shutdownGracefully()
            return nil
        } catch {
            return error
        }
    }

    static func makeUpstreamConfiguration(
        upstreamPolicy: ProxyServer.UpstreamPolicy,
        applicationProtocols: [String]
    ) throws -> TLSConfiguration {
        var configuration = TLSConfiguration.makeClientConfiguration()
        configuration.applicationProtocols = applicationProtocols
        if !upstreamPolicy.verifyCertificate {
            configuration.certificateVerification = .none
        }
        if !upstreamPolicy.additionalTrustRootsPEM.isEmpty {
            let certificates = try upstreamPolicy.additionalTrustRootsPEM.flatMap {
                try NIOSSLCertificate.fromPEMBytes(Array($0.utf8))
            }
            configuration.additionalTrustRoots = [.certificates(certificates)]
        }
        return configuration
    }
}

enum ProxyTLSRuntimeError: Error, Equatable, Sendable {
    case unsupportedUpstreamProtocolOffer
}

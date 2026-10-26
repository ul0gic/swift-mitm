import NIOCore
import NIOPosix
import NIOSSL
import NIOTLS
import X509
import XCTest

@testable import SwiftMITM

final class TLSInterceptionTests: XCTestCase {
    func testFixedVerifiedIdentityAndALPNNegotiation() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ca = try CertificateAuthority()
        let runtime = try await ProxyTLSRuntime.make(authority: ca, upstreamPolicy: .default)
        do {
            let (negotiatedClientALPN, negotiatedServerALPN) = try await negotiateSNI(
                group: group,
                ca: ca,
                runtime: runtime
            )
            XCTAssertEqual(negotiatedClientALPN, "h2")
            XCTAssertEqual(negotiatedServerALPN, "h2")
            let runtimeError = await runtime.shutdownGracefully()
            XCTAssertNil(runtimeError)
            try await group.shutdownGracefully()
        } catch {
            _ = await runtime.shutdownGracefully()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testNoSNIFallbackUsesTunnelAuthorityLeaf() throws {
        let ca = try CertificateAuthority()
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let pool = NIOThreadPool(numberOfThreads: 1)
        pool.start()
        defer { try? pool.syncShutdownGracefully() }
        let identities = LeafIdentityCache(
            authority: ca,
            threadPool: pool,
            maximumEntries: 2,
            maximumPendingMints: 2
        )
        let loop = group.next()
        let baseIdentity = try identities.identity(forHost: "api.example.com", on: loop).wait()
        let configuration = TLSTermination().makeServerConfiguration(baseIdentity: baseIdentity)
        XCTAssertNil(configuration.sslContextCallback)
        let leaf = try certificate(from: try XCTUnwrap(configuration.certificateChain.first))
        let san = try XCTUnwrap(leaf.extensions.subjectAlternativeNames)
        XCTAssertTrue(san.contains(.dnsName("api.example.com")))
        XCTAssertFalse(san.contains(.dnsName("localhost")))
    }

    func testAdditionalTrustRootsAugmentDefaultRoots() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let authority = try CertificateAuthority()
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: NoopSink(),
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [authority.caCertificatePEM])
        )

        let configuration = try proxy.makeUpstreamConfiguration(alpn: "http/1.1")

        XCTAssertEqual(configuration.trustRoots, .default)
        XCTAssertEqual(configuration.additionalTrustRoots.count, 1)
        guard case let .certificates(certificates) = configuration.additionalTrustRoots[0] else {
            return XCTFail("additional PEM roots must remain additional certificates")
        }
        XCTAssertEqual(certificates.count, 1)
    }

    private func negotiateSNI(
        group: EventLoopGroup,
        ca: CertificateAuthority,
        runtime: ProxyTLSRuntime
    ) async throws -> (String?, String?) {
        let serverContext = try await runtime
            .makeServerContext(
                defaultHost: "example.com",
                applicationProtocols: [.http2, .http11],
                on: group.next()
            )
            .get()
        let serverALPN = group.next().makePromise(of: String?.self)
        let server = try await ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(NIOSSLServerHandler(context: serverContext))
                    try channel.pipeline.syncOperations.addHandler(HandshakeProbe(promise: serverALPN))
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        let port = try XCTUnwrap(server.localAddress?.port)

        var clientConfig = TLSConfiguration.makeClientConfiguration()
        clientConfig.trustRoots = .certificates([
            try NIOSSLCertificate(bytes: Array(ca.caCertificatePEM.utf8), format: .pem)
        ])
        clientConfig.certificateVerification = .fullVerification
        clientConfig.applicationProtocols = ["h2", "http/1.1"]
        let clientContext = try NIOSSLContext(configuration: clientConfig)
        let clientALPN = group.next().makePromise(of: String?.self)
        let client = try await ClientBootstrap(group: group)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        NIOSSLClientHandler(context: clientContext, serverHostname: "example.com")
                    )
                    try channel.pipeline.syncOperations.addHandler(HandshakeProbe(promise: clientALPN))
                }
            }
            .connect(host: "127.0.0.1", port: port)
            .get()
        let result = try await (clientALPN.futureResult.get(), serverALPN.futureResult.get())
        try await client.close().get()
        try await server.close().get()
        return result
    }

    private func certificate(from source: NIOSSLCertificateSource) throws -> Certificate {
        guard case let .certificate(certificate) = source else {
            throw TLSInterceptionTestError.unexpectedCertificateSource
        }
        return try Certificate(derEncoded: certificate.toDERBytes())
    }

    private struct NoopSink: CaptureEventSink {
        func receive(_ event: CaptureEvent) {}
    }
}

private enum TLSInterceptionTestError: Error {
    case unexpectedCertificateSource
}

private final class HandshakeProbe: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let promise: EventLoopPromise<String?>

    init(promise: EventLoopPromise<String?>) {
        self.promise = promise
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case let TLSUserEvent.handshakeCompleted(negotiatedProtocol) = event {
            promise.succeed(negotiatedProtocol)
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        promise.fail(error)
        context.close(promise: nil)
    }
}

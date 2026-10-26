import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class ProxyServerTests: XCTestCase {
    private struct NoopSink: CaptureEventSink {
        func receive(_ event: CaptureEvent) {}
    }

    func testStartReturnsBoundPortAndStops() async throws {
        let ca = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink())
        let port = try await proxy.start(port: 0)
        XCTAssertGreaterThan(port, 0)
        try await proxy.stop()
    }

    func testRepeatedStartRejectsSecondListener() async throws {
        let ca = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink())
        _ = try await proxy.start(port: 0)

        do {
            _ = try await proxy.start(port: 0)
            XCTFail("a running proxy must reject a second start")
        } catch ProxyServerError.alreadyRunning {}

        try await proxy.stop()
    }

    func testOwnedGroupStopIsIdempotentAndTerminal() async throws {
        let ca = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink())
        _ = try await proxy.start(port: 0)

        try await proxy.stop()
        try await proxy.stop()

        do {
            _ = try await proxy.start(port: 0)
            XCTFail("a proxy cannot restart its shutdown event-loop group")
        } catch ProxyServerError.eventLoopGroupShutdown {}
    }

    func testInjectedGroupRemainsOwnedByConsumerAndSupportsRestart() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ca = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink(), group: group)

        do {
            _ = try await proxy.start(port: 0)
            try await proxy.stop()
            _ = try await proxy.start(port: 0)
            try await proxy.stop()
            try await proxy.stop()
        } catch {
            try? await group.shutdownGracefully()
            throw error
        }
        try await group.shutdownGracefully()
    }

    func testStopClosesActiveAcceptedConnectionBeforeReturning() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ca = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink(), group: group)

        do {
            let port = try await proxy.start(port: 0)
            let client = try await ClientBootstrap(group: group)
                .connect(host: "127.0.0.1", port: port)
                .get()
            XCTAssertTrue(client.isActive)

            try await proxy.stop()

            try await client.closeFuture.get()
            XCTAssertFalse(client.isActive)

            let restartedPort = try await proxy.start(port: 0)
            let restartedClient = try await ClientBootstrap(group: group)
                .connect(host: "127.0.0.1", port: restartedPort)
                .get()
            XCTAssertTrue(restartedClient.isActive)
            try await proxy.stop()
            try await restartedClient.closeFuture.get()
            XCTAssertFalse(restartedClient.isActive)
        } catch {
            try? await proxy.stop()
            try? await group.shutdownGracefully()
            throw error
        }
        try await group.shutdownGracefully()
    }

    func testStopClosesHeldHTTP1ClientAndUpstreamConnections() async throws {
        try await assertStopClosesHeldConnections(alpn: "http/1.1")
    }

    func testStopClosesHeldHTTP2ClientAndUpstreamConnections() async throws {
        try await assertStopClosesHeldConnections(alpn: "h2")
    }

    func testNonLoopbackBindRejectedWithoutOptIn() async throws {
        let ca = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink())
        do {
            _ = try await proxy.start(host: "0.0.0.0", port: 0)
            XCTFail("non-loopback bind must be rejected without explicit opt-in")
        } catch ProxyServerError.nonLoopbackBindRejected {}
    }

    func testHTTP1ReoriginationDeliversBodyAndEmitsCaptureEvents() async throws {
        try await runReorigination(alpn: "http/1.1", bodySize: 200_000)
    }

    func testHTTP2ReoriginationDeliversBodyAndEmitsCaptureEvents() async throws {
        try await runReorigination(alpn: "h2", bodySize: 200_000)
    }

    func testUnsupportedClientALPNClosesWithoutContactingOrigin() async throws {
        let traffic = MultiThreadedEventLoopGroup.singleton
        let origin = try TLSOriginServer(group: traffic, bodySize: 0)
        try origin.start()
        defer { origin.stop() }
        let mitmCA = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: NoopSink(),
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )

        do {
            let proxyPort = try await proxy.start(port: 0)
            let originHost = origin.hostname
            let originPort = origin.localPort
            let mitmCACertificatePEM = mitmCA.caCertificatePEM
            do {
                _ = try await runBlocking {
                    try ProxyTestClient(group: traffic).fetch(
                        proxyPort: proxyPort,
                        originHost: originHost,
                        originPort: originPort,
                        mitmCACertificatePEM: mitmCACertificatePEM,
                        applicationProtocols: ["unsupported"],
                        expectedALPN: "unsupported"
                    )
                }
                XCTFail("unsupported client ALPN must fail")
            } catch {}
            XCTAssertTrue(origin.activeChildChannels.isEmpty)
            try await proxy.stop()
        } catch {
            try? await proxy.stop()
            throw error
        }
    }

    func testStopClosesConnectionsWhileOriginALPNNegotiationIsInFlight() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 0, performsTLSHandshake: false)
        try origin.start()
        let mitmCA = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: NoopSink(),
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )

        do {
            let proxyPort = try await proxy.start(port: 0)
            let originHost = origin.hostname
            let originPort = origin.localPort
            let mitmCACertificatePEM = mitmCA.caCertificatePEM
            let client = try await runBlocking {
                try ProxyTestClient(group: group).beginTLSHandshake(
                    proxyPort: proxyPort,
                    originHost: originHost,
                    originPort: originPort,
                    mitmCACertificatePEM: mitmCACertificatePEM,
                    applicationProtocols: ["h2", "http/1.1"]
                )
            }
            try await waitUntil { !origin.activeChildChannels.isEmpty }
            let upstream = try XCTUnwrap(origin.activeChildChannels.first)

            try await proxy.stop()

            try await client.closeFuture.get()
            try await upstream.closeFuture.get()
            XCTAssertFalse(client.isActive)
            XCTAssertFalse(upstream.isActive)
        } catch {
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
        origin.stop()
        try await group.shutdownGracefully()
    }

    private func runReorigination(alpn: String, bodySize: Int) async throws {
        try await runReorigination(
            originProtocols: ["h2", "http/1.1"],
            clientProtocols: [alpn],
            expectedALPN: alpn,
            bodySize: bodySize
        )
    }

    private func runReorigination(
        originProtocols: [String],
        clientProtocols: [String],
        expectedALPN: String,
        bodySize: Int
    ) async throws {
        let traffic = MultiThreadedEventLoopGroup.singleton

        let origin = try TLSOriginServer(
            group: traffic,
            bodySize: bodySize,
            applicationProtocols: originProtocols
        )
        try origin.start()
        defer { origin.stop() }

        let mitmCA = try CertificateAuthority.generate().authority
        let sink = CountingSink()
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )
        let proxyPort = try await proxy.start(port: 0)

        let originHost = origin.hostname
        let originPort = origin.localPort
        let mitmPEM = mitmCA.caCertificatePEM
        let received = try await runBlocking {
            try ProxyTestClient(group: traffic).fetch(
                proxyPort: proxyPort,
                originHost: originHost,
                originPort: originPort,
                mitmCACertificatePEM: mitmPEM,
                applicationProtocols: clientProtocols,
                expectedALPN: expectedALPN
            )
        }

        XCTAssertEqual(received, bodySize, "client did not receive the full body over \(expectedALPN)")
        try await waitUntil { sink.capturedResponseBytes == bodySize }
        XCTAssertGreaterThan(sink.capturedRequestHeads, 0)
        XCTAssertGreaterThan(sink.capturedResponseHeads, 0)
        XCTAssertEqual(sink.capturedResponseBytes, bodySize)
        XCTAssertEqual(sink.capturedErrors, 0)

        try await proxy.stop()
    }

    private func assertStopClosesHeldConnections(alpn: String) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 0)
        try origin.start()
        let mitmCA = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: NoopSink(),
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )

        do {
            let proxyPort = try await proxy.start(port: 0)
            let originHost = origin.hostname
            let originPort = origin.localPort
            let mitmCACertificatePEM = mitmCA.caCertificatePEM
            let client = try await runBlocking {
                try ProxyTestClient(group: group).holdConnection(
                    proxyPort: proxyPort,
                    originHost: originHost,
                    originPort: originPort,
                    mitmCACertificatePEM: mitmCACertificatePEM,
                    alpn: alpn
                )
            }
            try await waitUntil { !origin.activeChildChannels.isEmpty }
            let upstream = try XCTUnwrap(origin.activeChildChannels.first)
            XCTAssertTrue(client.isActive)
            XCTAssertTrue(upstream.isActive)

            try await proxy.stop()

            try await client.closeFuture.get()
            try await upstream.closeFuture.get()
            XCTAssertFalse(client.isActive)
            XCTAssertFalse(upstream.isActive)
            let groupProbe = try await group.next().submit { 42 }.get()
            XCTAssertEqual(groupProbe, 42)
        } catch {
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
        origin.stop()
        try await group.shutdownGracefully()
    }

    private func runBlocking<T: Sendable>(_ work: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try work() })
            }
        }
    }

    private func waitUntil(
        timeout: TimeInterval = 5,
        _ condition: @escaping () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if condition() {
                return
            }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertTrue(condition(), "condition not met within \(timeout)s")
    }
}

extension ProxyServerTests {
    func testHTTP2CapableClientUsesHTTP1WhenOriginOnlySupportsHTTP1() async throws {
        try await runReorigination(
            originProtocols: ["http/1.1"],
            clientProtocols: ["h2", "http/1.1"],
            expectedALPN: "http/1.1",
            bodySize: 200_000
        )
    }

    func testHTTP2CapableClientUsesHTTP1FallbackWhenOriginDoesNotNegotiateALPN() async throws {
        try await runReorigination(
            originProtocols: [],
            clientProtocols: ["h2", "http/1.1"],
            expectedALPN: "http/1.1",
            bodySize: 200_000
        )
    }
}

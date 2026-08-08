import Foundation
import NIOPosix
import XCTest

@testable import SwiftMITM

final class ProxyServerTests: XCTestCase {
    private struct NoopSink: CaptureEventSink {
        func receive(_ event: CaptureEvent) {}
    }

    func testStartReturnsBoundPortAndStops() async throws {
        let ca = try CertificateAuthority()
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink())
        let port = try await proxy.start(port: 0)
        XCTAssertGreaterThan(port, 0)
        try await proxy.stop()
    }

    func testNonLoopbackBindRejectedWithoutOptIn() async throws {
        let ca = try CertificateAuthority()
        let proxy = ProxyServer(certificateAuthority: ca, sink: NoopSink())
        do {
            _ = try await proxy.start(host: "0.0.0.0", port: 0)
            XCTFail("non-loopback bind must be rejected without explicit opt-in")
        } catch ProxyServerError.nonLoopbackBindRejected {
            // expected: the guard throws before any socket is bound
        }
    }

    func testHTTP1ReoriginationDeliversBodyAndEmitsCaptureEvents() async throws {
        try await runReorigination(alpn: "http/1.1", bodySize: 200_000)
    }

    func testHTTP2ReoriginationDeliversBodyAndEmitsCaptureEvents() async throws {
        try await runReorigination(alpn: "h2", bodySize: 200_000)
    }

    private func runReorigination(alpn: String, bodySize: Int) async throws {
        let traffic = MultiThreadedEventLoopGroup.singleton

        let origin = try TLSOriginServer(group: traffic, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }

        let mitmCA = try CertificateAuthority()
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
                alpn: alpn
            )
        }

        XCTAssertEqual(received, bodySize, "client did not receive the full body over \(alpn)")
        try await waitUntil { sink.capturedResponseBytes == bodySize }
        XCTAssertGreaterThan(sink.capturedRequestHeads, 0)
        XCTAssertGreaterThan(sink.capturedResponseHeads, 0)
        XCTAssertEqual(sink.capturedResponseBytes, bodySize)
        XCTAssertEqual(sink.capturedErrors, 0)

        try await proxy.stop()
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
            if condition() { return }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertTrue(condition(), "condition not met within \(timeout)s")
    }
}

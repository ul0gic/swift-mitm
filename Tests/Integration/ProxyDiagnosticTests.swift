import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class ProxyDiagnosticTests: XCTestCase {
    private func runTransfer(bodySize: Int) async throws -> Int {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let origin = try TLSOriginServer(group: group, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }

        let sink = CountingSink()
        let authority = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )
        let proxyPort = try await proxy.start(port: 0)
        do {
            let received = try ProxyTestClient(group: group).fetch(
                proxyPort: proxyPort,
                originHost: origin.hostname,
                originPort: origin.localPort,
                mitmCACertificatePEM: authority.caCertificatePEM,
                alpn: "h2"
            )
            try await proxy.stop()
            return received
        } catch {
            try? await proxy.stop()
            throw error
        }
    }

    func testTiny1KB() async throws {
        let received = try await runTransfer(bodySize: 1024)
        XCTAssertEqual(received, 1024)
    }

    func testTwoWindows128KB() async throws {
        let received = try await runTransfer(bodySize: 128 * 1024)
        XCTAssertEqual(received, 128 * 1024)
    }

    func testOneMB() async throws {
        let received = try await runTransfer(bodySize: 1024 * 1024)
        XCTAssertEqual(received, 1024 * 1024)
    }

    func testSixteenMB() async throws {
        let received = try await runTransfer(bodySize: 16 * 1024 * 1024)
        XCTAssertEqual(received, 16 * 1024 * 1024)
    }
}

import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class UpstreamConnectionAdmissionTests: XCTestCase {
    func testInterceptedTLSKeepsWinningConnectionCharged() async throws {
        try await checkAdmission(passThrough: false)
    }

    func testRawPassThroughKeepsWinningConnectionCharged() async throws {
        try await checkAdmission(passThrough: true)
    }

    private func checkAdmission(passThrough: Bool) async throws {
        let authority = try CertificateAuthority.generate().authority
        let admission = ResourceAdmissionController(
            configuration: try ResourceAdmissionConfiguration(maximumConnectionsPerDestination: 1)
        )
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 8, applicationProtocols: ["http/1.1"])
        let events = Release022Events()
        var proxy: ProxyServer?
        do {
            try origin.start(bindAddress: "127.0.0.1")
            let originPort = origin.localPort
            let policy = try XCTUnwrap(ProxyInterceptionPolicy(
                passThroughAuthorities: passThrough ? ["localhost:\(originPort)"] : []
            ))
            let server = ProxyServer(
                certificateAuthority: authority,
                sink: events,
                group: group,
                upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
                egressPolicy: .init(allowInternal: true),
                interceptionPolicy: policy,
                admission: admission
            )
            proxy = server
            let proxyPort = try await server.start(port: 0)
            let client = ProxyTestClient(group: group)
            func connectedTunnel() throws -> Channel {
                let tunnel = try client.openTunnel(proxyPort: proxyPort, originHost: "localhost", originPort: originPort)
                do {
                    let tls = try client.installTLS(
                        on: tunnel,
                        serverHostname: "localhost",
                        mitmCACertificatePEM: passThrough ? origin.caCertificatePEM : authority.caCertificatePEM,
                        applicationProtocols: ["http/1.1"]
                    )
                    try phase4BoundedWait(tls.installed)
                    guard try phase4BoundedWait(tls.handshake) == "http/1.1" else { throw ProxyTestError.unexpectedALPN }
                    return tunnel
                } catch {
                    tunnel.close(promise: nil)
                    throw error
                }
            }
            func expectRejection(line: UInt = #line) throws {
                events.reset()
                do {
                    let unexpected = try connectedTunnel()
                    unexpected.close(promise: nil)
                    XCTFail("Unexpectedly established a TLS connection", line: line)
                } catch Phase2FixtureError.deadlineExceeded {
                    XCTFail("Timed out waiting for the rejected tunnel to close", line: line)
                    throw Phase2FixtureError.deadlineExceeded
                } catch {
                    try events.wait { captured in
                        captured.contains {
                            guard case let .connectionFailure(failure) = $0 else { return false }
                            return failure.reason == .upstreamConnectionFailed
                        }
                    }
                }
            }
            let winner = try connectedTunnel()
            XCTAssertEqual(admission.usage.connections, 2)
            try expectRejection()
            try await waitForConnections(2, admission: admission)
            XCTAssertTrue(winner.isActive)
            XCTAssertEqual(origin.activeChildChannels.count, 1)
            try await winner.close().get()
            try await waitForConnections(0, admission: admission)

            let next = try connectedTunnel()
            try await next.close().get()
            try await waitForConnections(0, admission: admission)
            origin.stop()
            try expectRejection()
            try await waitForConnections(0, admission: admission)
            try await server.stop()
        } catch {
            try? await proxy?.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
        try await group.shutdownGracefully()
    }

    private func waitForConnections(_ count: Int, admission: ResourceAdmissionController) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(3))
        while admission.usage.connections != count {
            guard ContinuousClock.now < deadline else {
                XCTAssertEqual(admission.usage.connections, count)
                throw Phase2FixtureError.deadlineExceeded
            }
            try await Task.sleep(for: .milliseconds(5))
        }
    }
}

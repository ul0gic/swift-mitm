import Foundation
import NIOPosix
import XCTest

import SwiftMITM

final class PublicProxyHTTP2SettingsWaitTests: XCTestCase {
    func testMissingOriginSettingsClosesTunnelAtProbeDeadline() async throws {
        let fixture = try Phase3SettingsWaitTestFixture()
        do {
            try fixture.origin.start()
            let proxyPort = try await fixture.proxy.start(port: 0)
            let clock = ContinuousClock()
            let start = clock.now
            let clientFailed = try await phase3RunBlocking {
                try fixture.client.waitForSettingsFailure(
                    proxyPort: proxyPort,
                    originHost: fixture.origin.hostname,
                    originPort: fixture.origin.localPort,
                    mitmCACertificatePEM: fixture.mitmCA.caCertificatePEM
                )
            }
            let elapsed = start.duration(to: clock.now)

            XCTAssertTrue(clientFailed)
            XCTAssertGreaterThanOrEqual(elapsed, .seconds(4))
            XCTAssertLessThan(elapsed, .seconds(7))
            try await fixture.stop()
        } catch {
            try? await fixture.stop()
            throw error
        }
    }

    func testStopDuringOriginSettingsWaitReturnsBeforeProbeDeadline() async throws {
        let fixture = try Phase3SettingsWaitTestFixture()
        do {
            try fixture.origin.start()
            let proxyPort = try await fixture.proxy.start(port: 0)
            async let clientFailed = phase3RunBlocking {
                try fixture.client.waitForSettingsFailure(
                    proxyPort: proxyPort,
                    originHost: fixture.origin.hostname,
                    originPort: fixture.origin.localPort,
                    mitmCACertificatePEM: fixture.mitmCA.caCertificatePEM
                )
            }
            try await phase3RunBlocking { try fixture.origin.receivedHTTP2Bytes.wait() }

            let clock = ContinuousClock()
            let start = clock.now
            try await fixture.proxy.stop()
            let elapsed = start.duration(to: clock.now)
            let didFail = try await clientFailed

            XCTAssertTrue(didFail)
            XCTAssertLessThan(elapsed, .seconds(2))
            fixture.client.stop()
            fixture.origin.stop()
            try await fixture.group.shutdownGracefully()
        } catch {
            try? await fixture.stop()
            throw error
        }
    }
}

private func phase3RunBlocking<T: Sendable>(
    _ operation: @escaping @Sendable () throws -> T
) async throws -> T {
    try await withCheckedThrowingContinuation { continuation in
        DispatchQueue.global().async {
            continuation.resume(with: Result { try operation() })
        }
    }
}

private final class Phase3SettingsWaitTestFixture: @unchecked Sendable {
    let group: MultiThreadedEventLoopGroup
    let origin: Phase3SilentSettingsTLSOrigin
    let mitmCA: CertificateAuthority
    let proxy: ProxyServer
    let client: Phase3ProxyHTTP2WebSocketClient

    init() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        self.group = group
        origin = try Phase3SilentSettingsTLSOrigin(group: group)
        mitmCA = try CertificateAuthority.generate().authority
        proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: Phase3RecordingSink(),
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )
        client = Phase3ProxyHTTP2WebSocketClient(group: group)
    }

    func stop() async throws {
        client.stop()
        try await proxy.stop()
        origin.stop()
        try await group.shutdownGracefully()
    }
}

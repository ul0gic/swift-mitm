import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Phase4PublicTimeoutTests: XCTestCase {
    func testTransparentTLSHandshakeDeadlineEmitsTimedOutFailureAndClosesDownstream() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Phase4CaptureSink()
        let origin = Phase4SilentRawOrigin(group: group)
        let proxy = try makeProxy(group: group, sink: sink)
        let forwarder = Phase4ProxyV2Forwarder(group: group)

        do {
            try origin.start()
            try await assertTLSHandshakeTimeout(
                proxy: proxy,
                origin: origin,
                forwarder: forwarder,
                sink: sink
            )
            forwarder.stop()
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            forwarder.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertTLSHandshakeTimeout(
        proxy: ProxyServer,
        origin: Phase4SilentRawOrigin,
        forwarder: Phase4ProxyV2Forwarder,
        sink: Phase4CaptureSink
    ) async throws {
        let proxyPort = try await proxy.start(port: 0)
        _ = try forwarder.connect(listenerPort: proxyPort)
        let downstreamInputClosed = try XCTUnwrap(forwarder.inputClosed)
        try forwarder.send(
            header: ingressHeader(destinationPort: origin.localPort),
            applicationBytes: Phase2TLSIngressVectors.http11ClientHello,
            delivery: .coalesced
        )
        try await origin.accepted.get()
        let snapshot: Phase4CaptureSink.Snapshot
        do {
            snapshot = try sink.wait(timeout: 1) { $0.connectionFailures.count == 1 }
        } catch {
            let current = sink.snapshot
            XCTFail(
                "timeout event missing failures=\(current.connectionFailures.map(\.reason)) "
                    + "events=\(current.eventKinds)"
            )
            throw error
        }
        do {
            try await downstreamInputClosed.get()
        } catch {
            XCTFail(
                "downstream EOF missing failures=\(snapshot.connectionFailures.map(\.reason)) "
                    + "events=\(snapshot.eventKinds)"
            )
            throw error
        }
        forwarder.stop()

        XCTAssertEqual(snapshot.connectionFailures.map(\.reason), [.timedOut])
        XCTAssertEqual(snapshot.eventKinds.filter { $0 == .connectionFailure }.count, 1)
        assertTarget(snapshot.connectionFailures.first?.target, originPort: origin.localPort)
        let clock = ContinuousClock()
        let started = clock.now
        try await proxy.stop()
        XCTAssertLessThan(started.duration(to: clock.now), .seconds(1))
    }

    private func makeProxy(group: EventLoopGroup, sink: Phase4CaptureSink) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        let timeoutPolicy = try XCTUnwrap(ProxyTimeoutPolicy(tlsHandshakeDeadline: .milliseconds(20)))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(verifyCertificate: false),
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            timeoutPolicy: timeoutPolicy
        )
    }

    private func ingressHeader(destinationPort: Int) -> Phase4ProxyV2Header {
        .ipv4(
            source: [192, 0, 2, 50],
            destination: [127, 0, 0, 1],
            sourcePort: 50_005,
            destinationPort: destinationPort
        )
    }

    private func assertTarget(_ target: CapturedTarget?, originPort: Int) {
        XCTAssertEqual(target?.destination.address, "127.0.0.1")
        XCTAssertEqual(target?.destination.port, originPort)
        XCTAssertEqual(target?.logicalAuthority, "127.0.0.1:\(originPort)")
        XCTAssertEqual(target?.tlsServerName, "localhost")
        XCTAssertEqual(target?.ingressProvenance, .trustedProxyV2)
        XCTAssertEqual(target?.originalClient?.address, "192.0.2.50")
        XCTAssertEqual(target?.originalClient?.port, 50_005)
    }
}

import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicTrustTests: XCTestCase {
    func testSNIIdentityAuthenticatesWithoutChangingCONNECTIPRoute() async throws {
        try await release022AssertIdentity(originName: "localhost", serverName: "localhost", succeeds: true)
    }

    func testTrustedRootWithWrongSNIIdentityFailsWithoutOpaqueFallback() async throws {
        try await release022AssertIdentity(originName: "localhost", serverName: "wrong.invalid", succeeds: false)
    }

    func testNoSNIAuthenticatesIPAddressSAN() async throws {
        try await release022AssertIdentity(originName: "127.0.0.1", serverName: nil, succeeds: true)
    }

    func testNoSNIRejectsTrustedDNSLeafForIPAddress() async throws {
        try await release022AssertIdentity(originName: "localhost", serverName: nil, succeeds: false)
    }

    func testSilentPostCONNECTExpiresAndReportsOneFailure() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Release022WireOrigin(group: group)
        let sink = Release022Events()
        let policy = try XCTUnwrap(ProxyTimeoutPolicy(tlsHandshakeDeadline: .milliseconds(250)))
        let proxy = ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            timeoutPolicy: policy
        )
        var client: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let clock = ContinuousClock()
            let start = clock.now
            let channel = try ProxyTestClient(group: group).openTunnel(
                proxyPort: port,
                originHost: "127.0.0.1",
                originPort: origin.port
            )
            client = channel
            try phase4BoundedWait(channel.closeFuture, timeout: .milliseconds(2250))
            XCTAssertLessThan(start.duration(to: clock.now), .milliseconds(2250))
            try await proxy.stop()
            let failures = sink.events.compactMap { event -> CapturedConnectionFailure? in
                if case let .connectionFailure(failure) = event {
                    failure
                } else {
                    nil
                }
            }
            XCTAssertEqual(failures.count, 1)
            XCTAssertEqual(failures.first?.reason, .timedOut)
            XCTAssertEqual(failures.first?.stage, .downstreamTLS)
            XCTAssertEqual(failures.first?.code, .setupTimedOut)
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testPolicySelectedTunnelKeepsOriginTLSAndReportsOpaqueReason() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 8, applicationProtocols: ["http/1.1"])
        let sink = Release022Events()
        var proxy: ProxyServer?
        do {
            try origin.start()
            let policy =
                try XCTUnwrap(ProxyInterceptionPolicy(passThroughAuthorities: ["localhost:\(origin.localPort)"]))
            let server = ProxyServer(
                certificateAuthority: try CertificateAuthority.generate().authority,
                sink: sink,
                group: group,
                egressPolicy: .init(allowInternal: true),
                interceptionPolicy: policy
            )
            proxy = server
            let port = try await server.start(port: 0)
            let count = try ProxyTestClient(group: group).fetch(
                proxyPort: port,
                originHost: "localhost",
                originPort: origin.localPort,
                mitmCACertificatePEM: origin.caCertificatePEM,
                alpn: "http/1.1"
            )
            XCTAssertEqual(count, 8)
            try await server.stop()
            assertPolicySelectedOpaqueFlow(sink.events)
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            if let proxy {
                try? await proxy.stop()
            }
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }
}

extension Release022PublicTrustTests {
    func testStopDuringHeldUpstreamHandshakeClosesBothPeersBeforeReturn() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Release022WireOrigin(group: group)
        let sink = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true)
        )
        var client: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let channel = try ProxyTestClient(group: group).openTunnel(
                proxyPort: port,
                originHost: "127.0.0.1",
                originPort: origin.port
            )
            client = channel
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: Phase2TLSIngressVectors.http11ClientHello)))
            _ = try origin.received.waitForBytes(1)
            let clock = ContinuousClock()
            let start = clock.now
            try await proxy.stop()
            XCTAssertLessThan(start.duration(to: clock.now), .seconds(2))
            try phase4BoundedWait(channel.closeFuture)
            try origin.received.waitForClose()
            let failures = sink.events.compactMap { event -> CapturedConnectionFailure? in
                if case let .connectionFailure(failure) = event {
                    failure
                } else {
                    nil
                }
            }
            XCTAssertEqual(failures.count, 1)
            XCTAssertEqual(failures.first?.code, .serverStopping)
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testDNSOnlyEgressDenialWinsOverSelectedPassThrough() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Release022WireOrigin(group: group)
        let sink = Release022Events()
        var proxy: ProxyServer?
        var client: Channel?
        do {
            try origin.start()
            let target = "localhost:\(origin.port)"
            let policy = try XCTUnwrap(ProxyInterceptionPolicy(passThroughAuthorities: [target]))
            let server = ProxyServer(
                certificateAuthority: try CertificateAuthority.generate().authority,
                sink: sink,
                group: group,
                interceptionPolicy: policy
            )
            proxy = server
            let port = try await server.start(port: 0)
            let received = Release022WireObservation()
            let channel = try release022WireClient(group: group, port: port, observation: received)
            client = channel
            try phase4BoundedWait(channel
                .writeAndFlush(ByteBuffer(string: "CONNECT \(target) HTTP/1.1\r\nHost: \(target)\r\n\r\n")))
            try received.waitForClose()
            try await server.stop()
            assertDeniedPassThrough(sink.events, acceptedConnections: origin.acceptedConnections)
            if channel.isActive {
                try await channel.close().get()
            }
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            if let proxy {
                try? await proxy.stop()
            }
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testTransparentPolicySelectedTunnelPreservesOriginTLS() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 8, applicationProtocols: ["http/1.1"])
        let sink = Release022Events()
        var proxy: ProxyServer?
        var client: Channel?
        do {
            try origin.start()
            let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
            let policy = try XCTUnwrap(ProxyInterceptionPolicy(
                passThroughAuthorities: ["127.0.0.1:\(origin.localPort)"]
            ))
            let server = ProxyServer(
                certificateAuthority: try CertificateAuthority.generate().authority,
                sink: sink,
                group: group,
                egressPolicy: .init(allowInternal: true),
                ingress: .trustedProxyV2(ingress),
                interceptionPolicy: policy
            )
            proxy = server
            let port = try await server.start(port: 0)
            let received = Release022WireObservation()
            let channel = try release022WireClient(group: group, port: port, observation: received)
            client = channel
            let header = Phase4ProxyV2Header.ipv4(
                source: [192, 0, 2, 1],
                destination: [127, 0, 0, 1],
                sourcePort: 50_001,
                destinationPort: origin.localPort
            )
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: header.bytes)))
            try fetchTransparentOrigin(channel: channel, group: group, origin: origin, received: received)
            try await channel.close().get()
            try await server.stop()
            assertTransparentPolicySelection(sink.events)
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            if let proxy {
                try? await proxy.stop()
            }
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }
}

extension Release022PublicTrustTests {
    private func assertPolicySelectedOpaqueFlow(_ events: [CaptureEvent]) {
        let flows = events.compactMap { event -> CapturedOpaqueFlow? in
            if case let .opaqueOpen(flow) = event {
                flow
            } else {
                nil
            }
        }
        XCTAssertEqual(flows.count, 1)
        XCTAssertEqual(flows.first?.reason, .policySelected)
        XCTAssertFalse(events.contains {
            if case .requestHead = $0 {
                true
            } else {
                false
            }
        })
        XCTAssertFalse(events.contains {
            if case .connectionFailure = $0 {
                true
            } else {
                false
            }
        })
    }

    private func assertDeniedPassThrough(_ events: [CaptureEvent], acceptedConnections: Int) {
        let failures = events.compactMap { event -> CapturedConnectionFailure? in
            if case let .connectionFailure(failure) = event {
                failure
            } else {
                nil
            }
        }
        XCTAssertEqual(failures.count, 1)
        XCTAssertEqual(failures.first?.code, .egressDenied)
        XCTAssertEqual(acceptedConnections, 0)
        XCTAssertFalse(events.contains {
            if case .opaqueOpen = $0 {
                true
            } else {
                false
            }
        })
    }

    private func fetchTransparentOrigin(
        channel: Channel,
        group: EventLoopGroup,
        origin: TLSOriginServer,
        received: Release022WireObservation
    ) throws {
        let tls = try ProxyTestClient(group: group).installTLS(
            on: channel,
            serverHostname: "localhost",
            mitmCACertificatePEM: origin.caCertificatePEM,
            applicationProtocols: ["http/1.1"]
        )
        try phase4BoundedWait(tls.installed)
        XCTAssertEqual(try phase4BoundedWait(tls.handshake), "http/1.1")
        try phase4BoundedWait(channel
            .writeAndFlush(ByteBuffer(string: "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")))
        _ = try received.wait { String(bytes: $0, encoding: .utf8)?.hasSuffix("AAAAAAAA") == true }
    }

    private func assertTransparentPolicySelection(_ events: [CaptureEvent]) {
        let reasons = events.compactMap { event -> OpaqueForwardingReason? in
            if case let .opaqueOpen(flow) = event {
                flow.reason
            } else {
                nil
            }
        }
        XCTAssertEqual(reasons, [.policySelected])
        XCTAssertFalse(events.contains {
            if case .requestHead = $0 {
                true
            } else {
                false
            }
        })
    }
}

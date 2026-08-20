import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Phase6LifecycleConcurrencyTests: XCTestCase {
    func testTransparentOwnedGroupStopClosesAcceptedConnectionAndIsTerminal() async throws {
        let clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let sink = Phase4CaptureSink()
        let proxy = try makeOwnedTransparentProxy(sink: sink)

        do {
            let port = try await proxy.start(port: 0)
            let client = try await ClientBootstrap(group: clientGroup)
                .connect(host: "127.0.0.1", port: port)
                .get()
            XCTAssertTrue(client.isActive)

            try await proxy.stop()
            try await client.closeFuture.get()
            try await proxy.stop()
            XCTAssertFalse(client.isActive)

            do {
                _ = try await proxy.start(port: 0)
                XCTFail("an owned event-loop group must be terminal after stop")
            } catch ProxyServerError.eventLoopGroupShutdown {}

            try await clientGroup.shutdownGracefully()
        } catch {
            try? await proxy.stop()
            try? await clientGroup.shutdownGracefully()
            throw error
        }
    }

    func testTransparentInjectedGroupRestartsAndRemainsConsumerOwned() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let sink = Phase4CaptureSink()
        let proxy = try makeInjectedTransparentProxy(group: group, sink: sink)

        do {
            let firstPort = try await proxy.start(port: 0)
            let firstClient = try await ClientBootstrap(group: group)
                .connect(host: "127.0.0.1", port: firstPort)
                .get()
            try await proxy.stop()
            try await firstClient.closeFuture.get()

            let secondPort = try await proxy.start(port: 0)
            let secondClient = try await ClientBootstrap(group: group)
                .connect(host: "127.0.0.1", port: secondPort)
                .get()
            try await proxy.stop()
            try await secondClient.closeFuture.get()

            let groupProbe = try await group.next().submit { 42 }.get()
            XCTAssertEqual(groupProbe, 42)
            try await group.shutdownGracefully()
        } catch {
            try? await proxy.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testConcurrentExplicitAndTransparentServersShareSinkAndEventLoopGroup() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let explicitOrigin = try TLSOriginServer(group: group, bodySize: 16, applicationProtocols: ["http/1.1"])
        let transparentOrigin = try TLSOriginServer(group: group, bodySize: 16, applicationProtocols: ["http/1.1"])
        let authority = try CertificateAuthority.generate().authority
        let sink = Phase4CaptureSink()
        let proxies = try makeConcurrentProxies(
            group: group,
            explicitOrigin: explicitOrigin,
            transparentOrigin: transparentOrigin,
            authority: authority,
            sink: sink
        )
        let explicitProxy = proxies.explicit
        let transparentProxy = proxies.transparent
        var adapter: Phase6IngressAdapter?

        do {
            try explicitOrigin.start()
            try transparentOrigin.start()
            let exchange = try await runConcurrentExchanges(
                group: group,
                explicitProxy: explicitProxy,
                transparentProxy: transparentProxy,
                origins: (explicitOrigin, transparentOrigin),
                authority: authority,
                sink: sink
            )
            adapter = exchange.adapter
            assertConcurrentCapture(exchange.snapshot)

            exchange.adapter.stop()
            try await explicitProxy.stop()
            try await transparentProxy.stop()
            explicitOrigin.stop()
            transparentOrigin.stop()
            let groupProbe = try await group.next().submit { 84 }.get()
            XCTAssertEqual(groupProbe, 84)
            try await group.shutdownGracefully()
        } catch {
            adapter?.stop()
            try? await explicitProxy.stop()
            try? await transparentProxy.stop()
            explicitOrigin.stop()
            transparentOrigin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testSimultaneousPeerClientAndProxyTerminationEmitsOneOpaqueTerminal() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let sink = Phase4CaptureSink()
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            classificationDeadline: .milliseconds(20)
        ))
        let proxy = ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            opaqueCaptureByteLimit: 1
        )
        let origin = Phase6TerminalPeer(group: group)
        let client = Phase6TerminalClient(group: group)
        var forwarder: Phase5GuestStyleForwarder?

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let guestForwarder = Phase5GuestStyleForwarder(
                group: group,
                proxyPort: proxyPort,
                destination: .init(family: .ipv4, address: "127.0.0.1", port: origin.localPort)
            )
            forwarder = guestForwarder
            try guestForwarder.start()
            try client.connect(port: guestForwarder.localPort, bytes: [0x01])
            try await origin.accepted.get()
            let opened = try sink.wait { $0.opaqueFlows.count == 1 }
            let flowID = try XCTUnwrap(opened.opaqueFlows.first?.id)

            async let clientClose: Void = Self.runBlocking { client.close() }
            async let originClose: Void = Self.runBlocking { origin.closeAll() }
            async let proxyClose: Void = proxy.stop()
            _ = try await (clientClose, originClose, proxyClose)

            assertSingleOpaqueTerminal(sink.snapshot, flowID: flowID)

            guestForwarder.stop()
            try await group.shutdownGracefully()
        } catch {
            client.close()
            forwarder?.stop()
            try? await proxy.stop()
            origin.closeAll()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func makeOwnedTransparentProxy(sink: Phase4CaptureSink) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            ingress: .trustedProxyV2(ingress)
        )
    }

    private func makeConcurrentProxies(
        group: EventLoopGroup,
        explicitOrigin: TLSOriginServer,
        transparentOrigin: TLSOriginServer,
        authority: CertificateAuthority,
        sink: Phase4CaptureSink
    ) throws -> (explicit: ProxyServer, transparent: ProxyServer) {
        let explicit = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [explicitOrigin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 16
        )
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        let transparent = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [transparentOrigin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            captureBodyLimit: 16
        )
        return (explicit, transparent)
    }

    private func runConcurrentExchanges(
        group: EventLoopGroup,
        explicitProxy: ProxyServer,
        transparentProxy: ProxyServer,
        origins: (explicit: TLSOriginServer, transparent: TLSOriginServer),
        authority: CertificateAuthority,
        sink: Phase4CaptureSink
    ) async throws -> (adapter: Phase6IngressAdapter, snapshot: Phase4CaptureSink.Snapshot) {
        let explicitPort = try await explicitProxy.start(port: 0)
        let transparentPort = try await transparentProxy.start(port: 0)
        let adapter = Phase6IngressAdapter(
            group: group,
            proxyPort: transparentPort,
            destinationPort: origins.transparent.localPort
        )
        do {
            try adapter.start()
            let explicitOriginPort = origins.explicit.localPort
            let transparentOriginPort = origins.transparent.localPort
            let transparentEntryPort = adapter.localPort
            let authorityPEM = authority.caCertificatePEM
            async let explicitBytes = Self.runBlocking {
                try ProxyTestClient(group: group).fetch(
                    proxyPort: explicitPort,
                    originHost: "localhost",
                    originPort: explicitOriginPort,
                    mitmCACertificatePEM: authorityPEM,
                    alpn: "http/1.1"
                )
            }
            async let transparentBytes = Self.runBlocking {
                try ProxyTestClient(group: group).fetch(
                    proxyPort: transparentEntryPort,
                    originHost: "localhost",
                    originPort: transparentOriginPort,
                    mitmCACertificatePEM: authorityPEM,
                    alpn: "http/1.1"
                )
            }
            let received = try await (explicitBytes, transparentBytes)
            XCTAssertEqual(received.0, 16)
            XCTAssertEqual(received.1, 16)
            let snapshot = try sink.wait { $0.requestHeads.count == 2 && $0.responseEnds.count == 2 }
            return (adapter, snapshot)
        } catch {
            adapter.stop()
            throw error
        }
    }

    private func assertConcurrentCapture(_ snapshot: Phase4CaptureSink.Snapshot) {
        XCTAssertEqual(snapshot.requestHeads.map(\.method), ["GET", "GET"])
        XCTAssertEqual(Set(snapshot.requestHeads.map { $0.target?.ingressProvenance.rawValue }), [
            CapturedIngressProvenance.explicitConnect.rawValue,
            CapturedIngressProvenance.trustedProxyV2.rawValue
        ])
        XCTAssertTrue(snapshot.connectionFailures.isEmpty)
    }

    private func assertSingleOpaqueTerminal(_ snapshot: Phase4CaptureSink.Snapshot, flowID: UUID) {
        let closeCount = snapshot.opaqueCloses.filter { $0.flowID == flowID }.count
        let errorCount = snapshot.opaqueErrors.filter { $0.flowID == flowID }.count
        XCTAssertEqual(closeCount + errorCount, 1)
        for direction in [OpaqueFlowDirection.clientToServer, .serverToClient] {
            let directionEnds = snapshot.opaqueDirectionEnds.filter {
                $0.flowID == flowID && $0.direction == direction
            }
            XCTAssertLessThanOrEqual(directionEnds.count, 1)
        }
    }

    private func makeInjectedTransparentProxy(
        group: EventLoopGroup,
        sink: Phase4CaptureSink
    ) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            ingress: .trustedProxyV2(ingress)
        )
    }

    private static func runBlocking<Value: Sendable>(
        _ operation: @escaping @Sendable () throws -> Value
    ) async throws -> Value {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

struct Phase6HTTPObservation: Equatable {
    let scheme: String
    let authority: String
    let method: String
    let path: String
    let version: HTTPProtocolVersion
    let requestHeaders: [HTTPHeaderField]
    let responseStatus: Int
    let responseVersion: HTTPProtocolVersion
    let responseHeaders: [HTTPHeaderField]
    let requestBytes: [UInt8]
    let requestByteCount: Int
    let responseBytes: [UInt8]
    let responseByteCount: Int
    let requestTruncated: Bool
    let responseTruncated: Bool
    let eventKinds: [Phase4CaptureSink.EventKind]

    init(snapshot: Phase4CaptureSink.Snapshot) throws {
        let request = try XCTUnwrap(snapshot.requestHeads.first)
        let response = try XCTUnwrap(snapshot.responseHeads.first)
        let requestEnd = try XCTUnwrap(snapshot.requestEnds.first)
        let responseEnd = try XCTUnwrap(snapshot.responseEnds.first)
        scheme = request.scheme
        authority = Self.normalizedAuthority(request.authority)
        method = request.method
        path = request.path
        version = request.version
        requestHeaders = Self.normalizedHeaders(request.headers)
        responseStatus = response.status
        responseVersion = response.version
        responseHeaders = response.headers
        requestBytes = snapshot.requestBodies.flatMap(\.bytes)
        requestByteCount = snapshot.requestBodies.reduce(0) { $0 + $1.byteCount }
        responseBytes = snapshot.responseBodies.flatMap(\.bytes)
        responseByteCount = snapshot.responseBodies.reduce(0) { $0 + $1.byteCount }
        requestTruncated = requestEnd.truncated
        responseTruncated = responseEnd.truncated
        eventKinds = Self.collapsed(snapshot.eventKinds)
    }

    private static func collapsed(_ eventKinds: [Phase4CaptureSink.EventKind]) -> [Phase4CaptureSink.EventKind] {
        eventKinds.reduce(into: []) { result, event in
            if result.last != event {
                result.append(event)
            }
        }
    }

    private static func normalizedAuthority(_ authority: String) -> String {
        authority.hasPrefix("localhost:") ? "localhost:<port>" : authority
    }

    private static func normalizedHeaders(_ headers: [HTTPHeaderField]) -> [HTTPHeaderField] {
        headers.map { header in
            HTTPHeaderField(name: header.name, value: normalizedAuthority(header.value))
        }
    }
}

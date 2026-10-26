import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Phase5GuestForwarderIntegrationTests: XCTestCase {
    func testRootlessGuestStyleForwarderPrependsConformantMetadataAndRelaysPublicTransparentHTTP() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Phase4ClearHTTPOrigin(group: group, scenario: .requestResponse)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink)
        let client = Phase4ClearHTTPClient(group: group)
        var forwarder: Phase5GuestStyleForwarder?

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let destination = Phase5ProxyV2Endpoint(
                family: .ipv4,
                address: "127.0.0.1",
                port: origin.localPort
            )
            let guestForwarder = Phase5GuestStyleForwarder(
                group: group,
                proxyPort: proxyPort,
                destination: destination
            )
            forwarder = guestForwarder
            try guestForwarder.start()

            let response = try client.exchange(port: guestForwarder.localPort, scenario: .requestResponse)
            let observedResponse = try await response.get()
            let forwarding = try await guestForwarder.observation.get()
            let originResult = try await origin.result.get()
            let snapshot = try sink.wait {
                $0.requestHeads.count == 1 && $0.responseHeads.count == 1 && $0.responseEnds.count == 1
            }

            XCTAssertEqual(observedResponse, Phase4ClearHTTPOrigin.responseBytes)
            XCTAssertEqual(originResult.requestBytes, Phase4ClearHTTPOrigin.requestBytes)
            XCTAssertFalse(originResult.requestBytes.starts(with: Array("CONNECT ".utf8)))
            XCTAssertEqual(forwarding.destination, destination)
            XCTAssertEqual(
                forwarding.headerBytes,
                try Phase5ProxyV2Encoder.encode(source: forwarding.source, destination: destination)
            )
            try assertCapture(snapshot, forwarding: forwarding)

            client.stop()
            guestForwarder.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            client.stop()
            forwarder?.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func makeProxy(group: EventLoopGroup, sink: Phase4CaptureSink) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            captureBodyLimit: 64
        )
    }

    private func assertCapture(
        _ snapshot: Phase4CaptureSink.Snapshot,
        forwarding: Phase5GuestStyleForwardingObservation
    ) throws {
        let request = try XCTUnwrap(snapshot.requestHeads.first)
        let target = try XCTUnwrap(request.target)

        XCTAssertEqual(request.scheme, "http")
        XCTAssertEqual(request.method, "GET")
        XCTAssertEqual(request.path, "/clear")
        XCTAssertEqual(request.version, .http11)
        XCTAssertEqual(snapshot.responseHeads.map(\.status), [200])
        XCTAssertEqual(snapshot.responseHeads.first?.requestID, request.id)
        XCTAssertEqual(snapshot.responseBodies.flatMap(\.bytes), Phase4ClearHTTPOrigin.responseBody)
        XCTAssertEqual(snapshot.responseEnds, [.init(requestID: request.id, truncated: false)])
        XCTAssertEqual(target.destination.address, forwarding.destination.address)
        XCTAssertEqual(target.destination.port, forwarding.destination.port)
        XCTAssertEqual(
            target.logicalAuthority,
            "\(forwarding.destination.address):\(forwarding.destination.port)"
        )
        XCTAssertNil(target.tlsServerName)
        XCTAssertEqual(target.ingressProvenance, .trustedProxyV2)
        XCTAssertEqual(target.originalClient?.address, forwarding.source.address)
        XCTAssertEqual(target.originalClient?.port, forwarding.source.port)
        XCTAssertTrue(snapshot.connectionFailures.isEmpty)
    }
}

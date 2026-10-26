import Foundation
import NIOPosix
import SwiftMITM
import XCTest

final class Phase7PublicAPIGateTests: XCTestCase {
    private final class SharedSink: CaptureEventSink {
        func receive(_: CaptureEvent) {}
    }

    func testTransparentIngressAndTimeoutPoliciesAreConsumerConstructible() throws {
        let peers = try XCTUnwrap(TrustedPeerPolicy(addressesAndCIDRs: ["127.0.0.1", "2001:db8::/32"]))
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: peers,
            proxyHeaderMaximumBytes: 8_192,
            proxyHeaderDeadline: .seconds(3),
            classificationMaximumBytes: 32_768,
            classificationDeadline: .milliseconds(750)
        ))
        let timeouts = try XCTUnwrap(ProxyTimeoutPolicy(
            upstreamConnectDeadline: .seconds(7),
            tlsHandshakeDeadline: .seconds(8),
            http2InitialSettingsDeadline: .seconds(4)
        ))

        XCTAssertEqual(peers.addressesAndCIDRs, ["127.0.0.1", "2001:db8::/32"])
        XCTAssertEqual(ingress.proxyHeaderMaximumBytes, 8_192)
        XCTAssertEqual(ingress.proxyHeaderDeadline, .seconds(3))
        XCTAssertEqual(ingress.classificationMaximumBytes, 32_768)
        XCTAssertEqual(ingress.classificationDeadline, .milliseconds(750))
        XCTAssertEqual(timeouts.upstreamConnectDeadline, .seconds(7))
        XCTAssertEqual(timeouts.tlsHandshakeDeadline, .seconds(8))
        XCTAssertEqual(timeouts.http2InitialSettingsDeadline, .seconds(4))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: []))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, proxyHeaderMaximumBytes: 15))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, classificationMaximumBytes: 65_537))
        XCTAssertNil(ProxyTimeoutPolicy(upstreamConnectDeadline: .zero))
        XCTAssertNil(ProxyTimeoutPolicy(tlsHandshakeDeadline: .seconds(-1)))
        XCTAssertNil(ProxyTimeoutPolicy(
            http2InitialSettingsDeadline: Duration(secondsComponent: Int64.max, attosecondsComponent: 0)
        ))
    }

    func testExplicitAndTransparentServersShareConsumerSinkAndGroup() async throws {
        let generated = try CertificateAuthority.generate(commonName: "Phase 7 Public API Root")
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        let sink = SharedSink()
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        addTeardownBlock { try await group.shutdownGracefully() }
        let explicit = ProxyServer(certificateAuthority: generated.authority, sink: sink, group: group)
        let transparent = ProxyServer(
            certificateAuthority: generated.authority,
            sink: sink,
            group: group,
            ingress: .trustedProxyV2(ingress)
        )

        let explicitPort = try await explicit.start(port: 0)
        let transparentPort = try await transparent.start(port: 0)
        XCTAssertGreaterThan(explicitPort, 0)
        XCTAssertGreaterThan(transparentPort, 0)
        XCTAssertNotEqual(explicitPort, transparentPort)
        try await explicit.stop()
        try await transparent.stop()
    }

    func testEveryCaptureEventCaseIsConsumerHandleable() {
        let id = UUID()
        let timestamp = Date(timeIntervalSince1970: 1_700_000_000)
        let target = makeTarget(provenance: .trustedProxyV2)
        let failure = CaptureEvent.connectionFailure(CapturedConnectionFailure(
            id: id,
            timestamp: timestamp,
            reason: .timedOut,
            target: target
        ))
        let events = makeHTTPEvents(id: id, timestamp: timestamp, target: target)
            + makeWebSocketEvents(id: id, timestamp: timestamp)
            + makeOpaqueEvents(id: id, timestamp: timestamp, target: target)
            + [failure]

        XCTAssertEqual(events.map(handleEveryCaptureEventCase), [
            "requestHead:trustedProxyV2", "requestBodyChunk", "requestTrailers", "requestEnd",
            "responseHead", "responseBodyChunk", "responseTrailers", "responseEnd", "streamError",
            "webSocketOpen", "webSocketFrame", "webSocketClose", "opaqueOpen:trustedProxyV2",
            "opaqueData:clientToServer", "opaqueDirectionEnd:serverToClient", "opaqueClose:completed",
            "opaqueError:transportFailure", "connectionFailure:timedOut"
        ])
        let targets = [
            makeTarget(provenance: .explicitConnect),
            makeTarget(provenance: .trustedProxyV2)
        ]
        XCTAssertEqual(targets.map(targetProvenance), ["explicitConnect", "trustedProxyV2"])
        XCTAssertEqual(allFailureReasons().map(handleEveryFailureReason), [
            "untrustedPeer", "malformedProxyMetadata", "unsupportedProxyTransport", "classificationFailed",
            "destinationUnavailable", "upstreamConnectionFailed", "tlsHandshakeFailed", "transportFailure",
            "timedOut", "cancelled"
        ])
    }

    private func makeTarget(provenance: CapturedIngressProvenance) -> CapturedTarget {
        CapturedTarget(
            destination: CapturedNetworkEndpoint(address: "192.0.2.10", port: 443),
            logicalAuthority: "example.com:443",
            tlsServerName: "example.com",
            ingressProvenance: provenance,
            originalClient: CapturedNetworkEndpoint(address: "192.0.2.20", port: 50_000)
        )
    }

    private func makeHTTPEvents(id: UUID, timestamp: Date, target: CapturedTarget) -> [CaptureEvent] {
        let headers = [HTTPHeaderField(name: "content-type", value: "application/octet-stream")]
        let request = CapturedRequestHead(
            id: id,
            timestamp: timestamp,
            scheme: "https",
            authority: "example.com",
            method: "GET",
            path: "/",
            version: .http2,
            headers: headers,
            target: target
        )
        let response = CapturedResponseHead(
            requestID: id, timestamp: timestamp, status: 200, version: .http2, headers: headers
        )
        return [
            .requestHead(request), .requestBodyChunk(requestID: id, bytes: [1], byteCount: 1),
            .requestTrailers(requestID: id, headers: headers), .requestEnd(requestID: id, truncated: false),
            .responseHead(response), .responseBodyChunk(requestID: id, bytes: [2], byteCount: 1),
            .responseTrailers(requestID: id, headers: headers), .responseEnd(requestID: id, truncated: false),
            .streamError(requestID: id, message: "closed")
        ]
    }

    private func makeWebSocketEvents(id: UUID, timestamp: Date) -> [CaptureEvent] {
        let frame = CapturedWebSocketFrame(
            connectionID: id,
            timestamp: timestamp,
            direction: .serverToClient,
            opcode: .text,
            fin: true,
            compressed: false,
            bytes: [3],
            byteCount: 1,
            truncated: false
        )
        return [
            .webSocketOpen(connectionID: id, timestamp: timestamp, permessageDeflate: false),
            .webSocketFrame(frame),
            .webSocketClose(connectionID: id, timestamp: timestamp, code: 1_000, reason: "complete")
        ]
    }

    private func makeOpaqueEvents(id: UUID, timestamp: Date, target: CapturedTarget) -> [CaptureEvent] {
        [
            .opaqueOpen(CapturedOpaqueFlow(id: id, timestamp: timestamp, target: target)),
            .opaqueData(flowID: id, timestamp: timestamp, direction: .clientToServer, bytes: [4], byteCount: 1),
            .opaqueDirectionEnd(
                flowID: id, timestamp: timestamp, direction: .serverToClient, byteCount: 1, truncated: false
            ),
            .opaqueClose(flowID: id, timestamp: timestamp, reason: .completed),
            .opaqueError(flowID: id, timestamp: timestamp, reason: .transportFailure)
        ]
    }

    // swiftlint:disable:next cyclomatic_complexity
    private func handleEveryCaptureEventCase(_ event: CaptureEvent) -> String {
        switch event {
        case .requestHead(let head): "requestHead:\(targetProvenance(head.target))"
        case .requestBodyChunk: "requestBodyChunk"
        case .requestTrailers: "requestTrailers"
        case .requestEnd: "requestEnd"
        case .responseHead: "responseHead"
        case .responseBodyChunk: "responseBodyChunk"
        case .responseTrailers: "responseTrailers"
        case .responseEnd: "responseEnd"
        case .streamError: "streamError"
        case .webSocketOpen: "webSocketOpen"
        case .webSocketFrame: "webSocketFrame"
        case .webSocketClose: "webSocketClose"
        case .opaqueOpen(let flow): "opaqueOpen:\(targetProvenance(flow.target))"
        case .opaqueData(_, _, let direction, _, _): "opaqueData:\(opaqueDirection(direction))"
        case .opaqueDirectionEnd(_, _, let direction, _, _): "opaqueDirectionEnd:\(opaqueDirection(direction))"
        case .opaqueClose(_, _, let reason): "opaqueClose:\(opaqueCloseReason(reason))"
        case .opaqueError(_, _, let reason): "opaqueError:\(handleEveryFailureReason(reason))"
        case .connectionFailure(let failure): "connectionFailure:\(handleEveryFailureReason(failure.reason))"
        @unknown default: "unknown"
        }
    }

    private func targetProvenance(_ target: CapturedTarget?) -> String {
        guard let target else { return "none" }
        return targetProvenance(target)
    }

    private func targetProvenance(_ target: CapturedTarget) -> String {
        switch target.ingressProvenance {
        case .explicitConnect: "explicitConnect"
        case .trustedProxyV2: "trustedProxyV2"
        @unknown default: "unknown"
        }
    }

    private func opaqueDirection(_ direction: OpaqueFlowDirection) -> String {
        switch direction {
        case .clientToServer: "clientToServer"
        case .serverToClient: "serverToClient"
        @unknown default: "unknown"
        }
    }

    private func opaqueCloseReason(_ reason: OpaqueFlowCloseReason) -> String {
        switch reason {
        case .completed: "completed"
        case .cancelled: "cancelled"
        @unknown default: "unknown"
        }
    }

    // swiftlint:disable:next cyclomatic_complexity
    private func handleEveryFailureReason(_ reason: CapturedConnectionFailureReason) -> String {
        switch reason {
        case .untrustedPeer: "untrustedPeer"
        case .malformedProxyMetadata: "malformedProxyMetadata"
        case .unsupportedProxyTransport: "unsupportedProxyTransport"
        case .classificationFailed: "classificationFailed"
        case .destinationUnavailable: "destinationUnavailable"
        case .upstreamConnectionFailed: "upstreamConnectionFailed"
        case .tlsHandshakeFailed: "tlsHandshakeFailed"
        case .transportFailure: "transportFailure"
        case .timedOut: "timedOut"
        case .cancelled: "cancelled"
        @unknown default: "unknown"
        }
    }

    private func allFailureReasons() -> [CapturedConnectionFailureReason] {
        [
            .untrustedPeer, .malformedProxyMetadata, .unsupportedProxyTransport, .classificationFailed,
            .destinationUnavailable, .upstreamConnectionFailed, .tlsHandshakeFailed, .transportFailure,
            .timedOut, .cancelled
        ]
    }
}

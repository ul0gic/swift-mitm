import Foundation
import NIOPosix
import SwiftMITM
import XCTest

final class PublicAPITests: XCTestCase {
    private struct NoopSink: CaptureEventSink {
        func receive(_ event: CaptureEvent) {}
    }

    func testCertificateAndProxyConfigurationAreConsumerAccessible() throws {
        let generated = try CertificateAuthority.generate()
        let restored = try CertificateAuthority(
            privateKeyPEM: generated.privateKeyPEM,
            certificatePEM: generated.certificatePEM
        )
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let proxy = ProxyServer(
            certificateAuthority: restored,
            sink: NoopSink(),
            group: group,
            upstreamPolicy: .init(),
            egressPolicy: .init(),
            captureBodyLimit: 0
        )

        XCTAssertEqual(CertificateAuthority.defaultCommonName, "SwiftMITM Root CA")
        XCTAssertFalse(restored.caCertificatePEM.isEmpty)
        withExtendedLifetime(proxy) {}
    }

    func testCaptureValuesAreConsumerConstructible() {
        let requestID = UUID()
        let timestamp = Date()
        let headers = [HTTPHeaderField(name: "content-type", value: "application/json")]
        let target = captureTarget()
        let request = CapturedRequestHead(
            id: requestID,
            timestamp: timestamp,
            scheme: "https",
            authority: "example.com",
            method: "GET",
            path: "/",
            version: .http2,
            headers: headers
        )
        let response = CapturedResponseHead(
            requestID: requestID,
            timestamp: timestamp,
            status: 200,
            version: .http2,
            headers: headers
        )
        let targetedRequest = CapturedRequestHead(
            id: requestID,
            timestamp: timestamp,
            scheme: "https",
            authority: "example.com",
            method: "GET",
            path: "/",
            version: .http2,
            headers: headers,
            target: target
        )
        let frame = capturedWebSocketFrame(requestID: requestID, timestamp: timestamp)
        let events: [CaptureEvent] = [
            .requestHead(request),
            .requestHead(targetedRequest),
            .requestTrailers(requestID: requestID, headers: headers),
            .responseHead(response),
            .responseTrailers(requestID: requestID, headers: headers),
            .webSocketFrame(frame)
        ] + opaqueEvents(requestID: requestID, timestamp: timestamp, target: target)

        XCTAssertNil(request.target)
        XCTAssertEqual(targetedRequest.target?.destination.address, "192.0.2.10")
        XCTAssertEqual(events.count, 12)
    }

    func testCaptureTargetAndFailureOptionalMetadataDefaultToNil() {
        let target = CapturedTarget(
            destination: CapturedNetworkEndpoint(address: "192.0.2.10", port: 443),
            logicalAuthority: "192.0.2.10:443",
            ingressProvenance: .explicitConnect
        )
        let failure = CapturedConnectionFailure(
            id: UUID(),
            timestamp: Date(),
            reason: .timedOut
        )

        XCTAssertNil(target.tlsServerName)
        XCTAssertNil(target.originalClient)
        XCTAssertNil(failure.target)
    }

    private func captureTarget() -> CapturedTarget {
        CapturedTarget(
            destination: CapturedNetworkEndpoint(address: "192.0.2.10", port: 443),
            logicalAuthority: "example.com:443",
            tlsServerName: "example.com",
            ingressProvenance: .trustedProxyV2,
            originalClient: CapturedNetworkEndpoint(address: "192.0.2.20", port: 50_000)
        )
    }

    private func capturedWebSocketFrame(requestID: UUID, timestamp: Date) -> CapturedWebSocketFrame {
        CapturedWebSocketFrame(
            connectionID: requestID,
            timestamp: timestamp,
            direction: .serverToClient,
            opcode: .text,
            fin: true,
            compressed: false,
            bytes: Array("ok".utf8),
            byteCount: 2,
            truncated: false
        )
    }

    private func opaqueEvents(
        requestID: UUID,
        timestamp: Date,
        target: CapturedTarget
    ) -> [CaptureEvent] {
        [
            .opaqueOpen(CapturedOpaqueFlow(id: requestID, timestamp: timestamp, target: target)),
            .opaqueData(
                flowID: requestID,
                timestamp: timestamp,
                direction: .clientToServer,
                bytes: [],
                byteCount: 4
            ),
            .opaqueDirectionEnd(
                flowID: requestID,
                timestamp: timestamp,
                direction: .clientToServer,
                byteCount: 4,
                truncated: true
            ),
            .opaqueClose(flowID: requestID, timestamp: timestamp, reason: .completed),
            .opaqueError(flowID: requestID, timestamp: timestamp, reason: .transportFailure),
            .connectionFailure(CapturedConnectionFailure(
                id: requestID,
                timestamp: timestamp,
                reason: .destinationUnavailable,
                target: target
            ))
        ]
    }
}

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
        let frame = CapturedWebSocketFrame(
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
        let events: [CaptureEvent] = [
            .requestHead(request),
            .requestTrailers(requestID: requestID, headers: headers),
            .responseHead(response),
            .responseTrailers(requestID: requestID, headers: headers),
            .webSocketFrame(frame)
        ]

        XCTAssertEqual(events.count, 5)
    }
}

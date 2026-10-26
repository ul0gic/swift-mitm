import Foundation
import NIOPosix
import XCTest

import SwiftMITM

final class PublicProxyHTTP2ConcurrentStreamTests: XCTestCase {
    func testOrdinaryAndWebSocketStreamsPreserveIdentityAndViolationIsolation() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Phase3ConcurrentHTTP2WebSocketOrigin(group: group)
        let mitmCA = try CertificateAuthority.generate().authority
        let sink = Phase3RecordingSink()
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 64
        )
        let client = Phase3ProxyHTTP2WebSocketClient(group: group)

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let clientResult = try await runBlocking {
                try client.concurrentExchange(
                    proxyPort: proxyPort,
                    originHost: origin.hostname,
                    originPort: origin.localPort,
                    mitmCACertificatePEM: mitmCA.caCertificatePEM
                )
            }
            let originResult = try await runBlocking { try origin.result.wait() }
            client.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
            assertResults(client: clientResult, origin: originResult, events: sink.events)
        } catch {
            client.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertResults(
        client: Phase3ConcurrentClientResult,
        origin: Phase3ConcurrentOriginResult,
        events: [CaptureEvent]
    ) {
        XCTAssertEqual(client.ordinaryResponseBytes, Array("ordinary-response".utf8))
        XCTAssertEqual(client.socketAResponseBytes, WebSocketWire.serverFrames)
        XCTAssertTrue(client.socketBFailedAfterAcceptance)
        XCTAssertEqual(origin.ordinaryRequestCount, 1)
        XCTAssertEqual(origin.socketABytes, WebSocketWire.clientFrames)
        XCTAssertEqual(origin.socketBBytes, WebSocketWire.clientTextFrame)
        XCTAssertTrue(origin.socketBTerminatedAfterViolation)
        assertCaptureIsolation(events)
    }

    private func assertCaptureIsolation(_ events: [CaptureEvent]) {
        let requests = events.compactMap { event -> CapturedRequestHead? in
            guard case .requestHead(let head) = event else { return nil }
            return head
        }
        XCTAssertEqual(Set(requests.map(\.path)), Set(["/ordinary", "/socket/a", "/socket/b"]))
        XCTAssertEqual(Set(requests.map(\.id)).count, 3)
        let ids = Dictionary(uniqueKeysWithValues: requests.map { ($0.path, $0.id) })
        let openIDs = events.compactMap { event -> UUID? in
            guard case .webSocketOpen(let connectionID, _, _) = event else { return nil }
            return connectionID
        }
        XCTAssertEqual(Set(openIDs), Set([ids["/socket/a"], ids["/socket/b"]].compactMap(\.self)))
        assertFrameIsolation(events, ids: ids)
        let errorIDs = events.compactMap { event -> UUID? in
            guard case .streamError(let requestID, _) = event else { return nil }
            return requestID
        }
        XCTAssertEqual(errorIDs, [ids["/socket/b"]].compactMap(\.self))
    }

    private func assertFrameIsolation(_ events: [CaptureEvent], ids: [String: UUID]) {
        let frames = events.compactMap { event -> CapturedWebSocketFrame? in
            guard case .webSocketFrame(let frame) = event else { return nil }
            return frame
        }
        XCTAssertEqual(frames.filter { $0.connectionID == ids["/socket/a"] }.map(\.bytes), [
            WebSocketWire.clientTextPayload,
            WebSocketWire.serverBinaryPayload,
            WebSocketWire.closePayload,
            WebSocketWire.closePayload
        ])
        XCTAssertEqual(frames.filter { $0.connectionID == ids["/socket/b"] }.map(\.bytes), [
            WebSocketWire.clientTextPayload
        ])
        XCTAssertTrue(frames.allSatisfy { $0.connectionID != ids["/ordinary"] })
    }

    private func runBlocking<T: Sendable>(
        _ operation: @escaping @Sendable () throws -> T
    ) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

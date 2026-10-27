import Foundation
import NIOCore
import XCTest

import SwiftMITM

final class Stage2ReplayBoundaryAcceptanceTests: XCTestCase {
    func testDroppedReplayResponseDoesNotRepeatTheCommittedPOST() async throws {
        try await stage2WithInspectionWire { peer in
            let request = ReplayRequest(
                source: ReplaySource(requestID: UUID(), representation: .original),
                destination: try XCTUnwrap(URL(string: "http://127.0.0.1:\(peer.origin.port)/committed")),
                method: "POST",
                headers: [HTTPHeaderField(name: "Content-Type", value: "text/plain")],
                body: Array("one-side-effect".utf8),
                bodyIsComplete: true
            )
            let proxy = peer.proxy
            let events = Stage2ReplayEvents()
            async let attempt = proxy.replay(request, responseSink: events)
            let committed = try peer.origin.received.wait {
                $0.suffix(request.body.count).elementsEqual(request.body)
            }
            let wire = try XCTUnwrap(String(bytes: committed, encoding: .utf8))
            XCTAssertTrue(wire.hasPrefix("POST /committed HTTP/1.1\r\n"))
            let origin = try peer.origin.waitForConnection(at: 0)
            try await origin.channel.close().get()
            do {
                _ = try await attempt
                XCTFail("Replay reported success after the origin dropped its response")
            } catch {
                XCTAssertFalse(error is CancellationError, "Peer failure required caller cancellation: \(error)")
            }
            XCTAssertEqual(peer.origin.acceptedConnections, 1)
            XCTAssertEqual(peer.origin.received.bytes, committed)
            XCTAssertFalse(events.events.contains { event in
                if case .end = event { true } else { false }
            })
        }
    }

    func testReplayRejectsSessionsAndLimitsCredentialOptInToSameOrigin() async throws {
        try await stage2WithHTTPOrigin { peer in
            let destination = try XCTUnwrap(URL(string: "http://127.0.0.1:\(peer.origin.port)/replayed"))
            let source = ReplaySource(requestID: UUID(), representation: .original)
            let unsupported: [(String, [HTTPHeaderField])] = [
                ("CONNECT", []),
                ("GET", [
                    HTTPHeaderField(name: "Connection", value: "Upgrade"),
                    HTTPHeaderField(name: "Upgrade", value: "websocket")
                ])
            ]
            for (method, headers) in unsupported {
                let request = ReplayRequest(
                    source: source,
                    destination: destination,
                    method: method,
                    headers: headers,
                    body: [],
                    bodyIsComplete: true
                )
                do {
                    _ = try await peer.proxy.replay(request, responseSink: Stage2ReplayEvents())
                    XCTFail("Unsupported session reached replay")
                } catch ReplayError.unsupportedSession {} catch { throw error }
            }
            XCTAssertEqual(peer.origin.acceptedConnections, 0)
            XCTAssertTrue(peer.origin.requests.isEmpty)
            let credentials = [
                HTTPHeaderField(name: "Authorization", value: "Bearer synthetic-same-origin"),
                HTTPHeaderField(name: "Cookie", value: "session=synthetic-same-origin"),
                HTTPHeaderField(name: "X-Private", value: "synthetic-custom")
            ]
            let sameOrigin = try XCTUnwrap(URL(string: "http://127.0.0.1:\(peer.origin.port)/captured"))
            let allowed = ReplayRequest(
                source: source,
                destination: destination,
                method: "POST",
                headers: credentials,
                body: Array("allowed".utf8),
                bodyIsComplete: true,
                credentialPolicy: .preserveForSameOrigin(sourceURL: sameOrigin),
                sensitiveHeaderNames: ["X-Private"]
            )
            let result = try await peer.proxy.replay(allowed, responseSink: Stage2ReplayEvents())
            XCTAssertEqual(result.status, 200)
            XCTAssertEqual(peer.origin.acceptedConnections, 1)
            XCTAssertEqual(peer.origin.requests.count, 1)
            let forwarded = try XCTUnwrap(peer.origin.requests.first)
            XCTAssertEqual(forwarded.body, allowed.body)
            for header in credentials {
                XCTAssertEqual(forwarded.head.headers.first(name: header.name), header.value)
            }
            let otherOrigin = try XCTUnwrap(URL(string: "https://127.0.0.1:\(peer.origin.port)/captured"))
            let denied = ReplayRequest(
                source: source,
                destination: destination,
                method: "POST",
                headers: credentials,
                body: Array("denied".utf8),
                bodyIsComplete: true,
                credentialPolicy: .preserveForSameOrigin(sourceURL: otherOrigin),
                sensitiveHeaderNames: ["X-Private"]
            )
            do {
                _ = try await peer.proxy.replay(denied, responseSink: Stage2ReplayEvents())
                XCTFail("Credentials crossed the source origin's scheme boundary")
            } catch ReplayError.credentialScopeMismatch {} catch { throw error }
            XCTAssertEqual(peer.origin.acceptedConnections, 1)
            XCTAssertEqual(peer.origin.requests.count, 1)
        }
    }
}

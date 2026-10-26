import Foundation
import NIOCore
import XCTest

import SwiftMITM

final class Release022PublicHTTP1OctetTests: XCTestCase {
    func testExplicitProxyRetainsObsTextFieldsAndTrailersAcrossKeepAlive() async throws {
        try await release022WithWire { origin, client, _, sink in
            let octets: [UInt8] = [0x80, 0xFF]
            let first = Array("POST http://127.0.0.1:\(origin.port)/octets HTTP/1.1\r\nHost: localhost\r\nX-Request: ".utf8)
                + octets + Array("\r\nTransfer-Encoding: chunked\r\nTrailer: X-Request-Trailer\r\n\r\n1\r\nz\r\n0\r\nX-Request-Trailer: ".utf8)
                + octets + Array("\r\n\r\n".utf8)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: first)))
            let upstream = try origin.received.wait {
                $0.suffix(4) == [13, 10, 13, 10]
                    && String(decoding: $0, as: UTF8.self).lowercased().contains("x-request-trailer:")
            }
            XCTAssertTrue(containsField("x-request", octets: octets, bytes: upstream))
            XCTAssertTrue(containsField("x-request-trailer", octets: octets, bytes: upstream))
            let response = Array("HTTP/1.1 200 OK\r\nX-Response: ".utf8) + octets
                + Array("\r\nTransfer-Encoding: chunked\r\nTrailer: X-Response-Trailer\r\n\r\n1\r\na\r\n0\r\nX-Response-Trailer: ".utf8)
                + octets + Array("\r\n\r\n".utf8)
            try origin.write(response)
            _ = try sink.wait { responseEnds($0).count == 1 }
            origin.received.reset()
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: "GET http://127.0.0.1:\(origin.port)/next HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try origin.write(Array("HTTP/1.1 204 No Content\r\n\r\n".utf8))
            let events = try sink.wait { responseEnds($0).count == 2 }
            let requests = events.compactMap { event -> CapturedRequestHead? in
                if case .requestHead(let head) = event {
                    head
                } else {
                    nil
                }
            }
            XCTAssertEqual(requests.map(\.path), ["/octets", "/next"])
            XCTAssertEqual(responseEnds(events), requests.map(\.id))
            XCTAssertEqual(requests.first?.headers.first { $0.name.lowercased() == "x-request" }?.value, "\u{0080}\u{00FF}")
            let responses = events.compactMap { event -> CapturedResponseHead? in
                if case .responseHead(let head) = event {
                    head
                } else {
                    nil
                }
            }
            XCTAssertEqual(responses.first?.headers.first { $0.name.lowercased() == "x-response" }?.value, "\u{0080}\u{00FF}")
            let requestTrailers = events.flatMap { event -> [HTTPHeaderField] in
                if case .requestTrailers(_, let headers) = event {
                    headers
                } else {
                    []
                }
            }
            let responseTrailers = events.flatMap { event -> [HTTPHeaderField] in
                if case .responseTrailers(_, let headers) = event {
                    headers
                } else {
                    []
                }
            }
            XCTAssertEqual(requestTrailers.first?.value, "\u{0080}\u{00FF}")
            XCTAssertEqual(responseTrailers.first?.value, "\u{0080}\u{00FF}")
            XCTAssertFalse(events.contains {
                if case .streamError = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    private func responseEnds(_ events: [CaptureEvent]) -> [UUID] {
        events.compactMap {
            if case .responseEnd(let id, _) = $0 {
                id
            } else {
                nil
            }
        }
    }

    private func containsField(_ name: String, octets: [UInt8], bytes: [UInt8]) -> Bool {
        let marker = Array((name + ": ").utf8) + octets + [13, 10]
        let folded = bytes.map { (65 ... 90).contains($0) ? $0 + 32 : $0 }
        guard folded.count >= marker.count else { return false }
        return (0 ... folded.count - marker.count).contains { Array(folded[$0 ..< $0 + marker.count]) == marker }
    }
}

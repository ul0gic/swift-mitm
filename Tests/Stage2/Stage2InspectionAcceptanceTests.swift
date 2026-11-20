import Foundation
import NIOCore
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2InspectionAcceptanceTests: XCTestCase {
    func testSSEEventRangesAcrossReadsAndGzipMembers() async throws {
        let first = Array("data: €\r\n\r".utf8)
        let second = Array("data: second\n\n".utf8)
        let bytes = first + [10] + second
        let ranges = [
            UInt64(0)..<UInt64(first.count),
            UInt64(first.count + 1)..<UInt64(bytes.count)
        ]
        let fragmented = [Array("data: ".utf8) + [0xE2], [0x82, 0xAC, 13], [10, 13], [10] + second]
        let gzipFirst = try XCTUnwrap(Data(base64Encoded: "H4sIAAAAAAAA/0tJLEm0UnjUtIaXi5cLACoKaJgNAAAA"))
        let gzipSecond = try XCTUnwrap(Data(base64Encoded: "H4sIAAAAAAAA/0tJLEm0UihOTc7PS+HiAgAOk9c8DgAAAA=="))
        let gzipRanges = [UInt64(0)..<UInt64(gzipFirst.count), UInt64(0)..<UInt64(gzipFirst.count + gzipSecond.count)]
        let deliveries = [
            (pieces: [bytes], codingHeader: "", wireRanges: ranges),
            (pieces: fragmented, codingHeader: "", wireRanges: ranges),
            (pieces: [Array(gzipFirst + gzipSecond)], codingHeader: "Content-Encoding: gzip\r\n", wireRanges: gzipRanges)
        ]
        for delivery in deliveries {
            try await stage2WithInspectionWire { peer in
                let request = "GET http://127.0.0.1:\(peer.origin.port)/events HTTP/1.1\r\nHost: localhost\r\n\r\n"
                try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
                _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
                let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n" + delivery.codingHeader
                    + "Transfer-Encoding: chunked\r\n\r\n"
                try peer.origin.write(Array(head.utf8))
                var sent = 0
                for piece in delivery.pieces {
                    try stage2WriteChunk(piece, origin: peer.origin)
                    sent += piece.count
                    _ = try peer.raw.wait { stage2ResponseCount($0) >= sent }
                    if sent == first.count {
                        _ = try await peer.inspected.wait { events in
                            events.contains { if case .sse = $0.payload { true } else { false } }
                        }
                    }
                }
                try peer.origin.write(Array("0\r\n\r\n".utf8))
                _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
                let drain = await peer.inspector.drain()
                XCTAssertTrue(drain.drained)
                let observed = peer.inspected.events.compactMap { event -> (String, InspectionEvent)? in
                    guard case let .sse(message) = event.payload else { return nil }
                    return (message.data, event)
                }
                XCTAssertEqual(observed.map { $0.0 }, ["€", "second"])
                XCTAssertEqual(observed.map { $0.1.decodedRange }, ranges)
                XCTAssertEqual(observed.map { $0.1.wireRange }, delivery.wireRanges)
            }
        }
    }

    func testSSEPublishesFragmentedUTF8BeforeEOFWithZeroRawRetention() async throws {
        try await stage2WithInspectionWire { peer in
            let request = "GET http://127.0.0.1:\(peer.origin.port)/events HTTP/1.1\r\nHost: localhost\r\n\r\n"
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
            _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try peer.origin.write(Array(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"
                    .utf8
            ))
            let pieces: [[UInt8]] = [
                [0xEF],
                [0xBB, 0xBF] + Array("id: evt-1\r".utf8),
                Array("\nevent: update\r\ndata: ".utf8) + [0xE2],
                [0x82],
                [0xAC] + Array("\r\ndata: second\r\n\r\n".utf8)
            ]
            var total = 0
            for piece in pieces {
                try stage2WriteChunk(piece, origin: peer.origin)
                total += piece.count
                _ = try peer.raw.wait { stage2ResponseCount($0) >= total }
            }
            let events = try await peer.inspected.wait { events in
                events.contains {
                    if case .sse = $0.payload {
                        true
                    } else {
                        false
                    }
                }
            }
            assertEarlySSE(events, raw: peer.raw.events)
            try stage2WriteChunk(Array("data: discarded-at-eof".utf8), origin: peer.origin)
            try peer.origin.write(Array("0\r\n\r\n".utf8))
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            let drain = await peer.inspector.drain()
            XCTAssertTrue(drain.drained)
            XCTAssertEqual(
                peer.inspected
                    .events
                    .filter {
                        if case .sse = $0.payload {
                            true
                        } else {
                            false
                        }
                    }
                    .count,
                1
            )
        }
    }
}

extension Stage2InspectionAcceptanceTests {
    private func assertEarlySSE(_ events: [InspectionEvent], raw: [CaptureEvent]) {
        let messages = events.compactMap { event -> SSEEvent? in
            if case let .sse(message) = event.payload {
                message
            } else {
                nil
            }
        }
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages.first?.data, "€\nsecond")
        XCTAssertEqual(messages.first?.event, "update")
        XCTAssertEqual(messages.first?.lastEventID, "evt-1")
        XCTAssertTrue(release022ResponseEnds(raw).isEmpty)
        XCTAssertTrue(release022ResponseBodyBytes(raw).isEmpty)
    }
}

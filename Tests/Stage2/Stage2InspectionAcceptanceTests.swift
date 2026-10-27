import Foundation
import NIOCore
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2InspectionAcceptanceTests: XCTestCase {
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

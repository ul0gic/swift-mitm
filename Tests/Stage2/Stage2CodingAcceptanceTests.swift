import Foundation
import NIOCore
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2CodingAcceptanceTests: XCTestCase {
    func testIndependentGzipDeflateAndConcatenatedOuterMembersPreserveDecodedEvents() async throws {
        let directory = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_COMPRESSION_VECTORS"])
        let root = URL(fileURLWithPath: directory)
        let expected = [UInt8](try Data(contentsOf: root.appendingPathComponent("plain.bin")))
        for (name, coding) in [("gzip.bin", "gzip"), ("deflate.bin", "deflate"), ("double-gzip.bin", "gzip, gzip")] {
            let encoded = [UInt8](try Data(contentsOf: root.appendingPathComponent(name)))
            let budget = try InspectionMemoryBudget()
            try await stage2WithInspectionWire(inspectionConfiguration: InspectionConfiguration(budget: budget)) { peer in
                try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string:
                    "GET http://127.0.0.1:\(peer.origin.port)/coding HTTP/1.1\r\nHost: localhost\r\n\r\n")))
                _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
                try peer.origin.write(Array((
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Encoding: \(coding)\r\n"
                    + "Content-Length: \(encoded.count)\r\n\r\n"
                ).utf8) + encoded)
                let forwarded = try peer.received.wait { $0.suffix(encoded.count).elementsEqual(encoded) }
                XCTAssertEqual(Array(forwarded.suffix(encoded.count)), encoded)
                _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
                let drained = await peer.inspector.drain()
                XCTAssertTrue(drained.drained)
                let decoded = peer.inspected.events.flatMap { event -> [UInt8] in
                    if case .decodedBody(let bytes) = event.payload, event.direction == .response { bytes } else { [] }
                }
                XCTAssertEqual(decoded, expected)
                let messages = peer.inspected.events.compactMap { event -> String? in
                    if case .sse(let message) = event.payload { message.data } else { nil }
                }
                XCTAssertEqual(messages, ["first", "second"])
                XCTAssertFalse(peer.inspected.events.contains { if case .diagnostic = $0.payload { true } else { false } })
                XCTAssertEqual(stage2ResponseCount(peer.raw.events), encoded.count)
                XCTAssertTrue(release022ResponseBodyBytes(peer.raw.events).isEmpty)
            }
            XCTAssertEqual(budget.retainedBytes, 0)
        }
    }

    func testSSEEventAtLimitAndPlusOneDoesNotTruncateTransport() async throws {
        for size in [4, 5] {
            let budget = try InspectionMemoryBudget()
            let configuration = try InspectionConfiguration(budget: budget, maximumLineBytes: 64, maximumEventBytes: 4)
            try await stage2WithInspectionWire(inspectionConfiguration: configuration) { peer in
                try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string:
                    "GET http://127.0.0.1:\(peer.origin.port)/limit HTTP/1.1\r\nHost: localhost\r\n\r\n")))
                _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
                let body = Array(("data: " + String(repeating: "x", count: size) + "\n\n").utf8)
                try peer.origin.write(Array((
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: \(body.count)\r\n\r\n"
                ).utf8) + body)
                let forwarded = try peer.received.wait { $0.suffix(body.count).elementsEqual(body) }
                XCTAssertEqual(Array(forwarded.suffix(body.count)), body)
                _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
                let drained = await peer.inspector.drain()
                XCTAssertTrue(drained.drained)
                let messages = peer.inspected.events.compactMap { event -> String? in
                    if case .sse(let message) = event.payload { message.data } else { nil }
                }
                if size == 4 {
                    XCTAssertEqual(messages, ["xxxx"])
                } else {
                    XCTAssertTrue(messages.isEmpty)
                    XCTAssertEqual(peer.inspected.events.filter { event in
                        if case .diagnostic(.malformedSSE(.eventLimitExceeded)) = event.payload { true } else { false }
                    }.count, 1)
                }
                XCTAssertTrue(release022Errors(peer.raw.events).isEmpty)
            }
            XCTAssertEqual(budget.retainedBytes, 0)
        }
    }
}

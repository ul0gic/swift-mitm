import Foundation
import NIOCore
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2SharedBudgetAcceptanceTests: XCTestCase {
    func testSharedInspectionBudgetExhaustionPreservesZeroRetentionForwardingAndReleasesCharges() async throws {
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration(
            maximumRetainedBytes: 65_536
        ))
        let budget = try InspectionMemoryBudget(maximumBytes: 1_048_576, admission: admission)
        let configuration = try InspectionConfiguration(
            budget: budget,
            maximumQueuedItems: 8,
            maximumActiveBodies: 2,
            maximumLineBytes: 262_144,
            maximumEventBytes: 262_144
        )
        try await stage2WithInspectionWire(inspectionConfiguration: configuration, admission: admission) { peer in
            XCTAssertGreaterThanOrEqual(admission.usage.retainedBytes, peer.inspector.reservedStorageBytes)
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/shared-limit HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let body = Array(("data: " + String(repeating: "x", count: 131_072) + "\n\n").utf8)
            try peer.origin.write(Array(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: \(body.count)\r\n\r\n"
                    .utf8
            ) + body)
            let forwarded = try peer.received.wait { $0.suffix(body.count).elementsEqual(body) }
            XCTAssertEqual(Array(forwarded.suffix(body.count)), body)
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            let drained = await peer.inspector.drain()
            XCTAssertTrue(drained.drained)
            XCTAssertTrue(peer.inspected.events.contains {
                if case .diagnostic = $0.payload {
                    true
                } else {
                    false
                }
            })
            XCTAssertFalse(peer.inspected.events.contains {
                if case .sse = $0.payload {
                    true
                } else {
                    false
                }
            })
            XCTAssertLessThanOrEqual(admission.usage.retainedBytes, admission.configuration.maximumRetainedBytes)
            XCTAssertEqual(stage2ResponseCount(peer.raw.events), body.count)
            XCTAssertTrue(release022ResponseBodyBytes(peer.raw.events).isEmpty)
            XCTAssertTrue(release022Errors(peer.raw.events).isEmpty)
        }
        XCTAssertEqual(budget.retainedBytes, 0)
        XCTAssertEqual(admission.usage.retainedBytes, 0)
    }
}

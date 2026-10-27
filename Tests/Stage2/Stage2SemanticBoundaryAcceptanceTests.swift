import Foundation
import NIOCore
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2SemanticBoundaryAcceptanceTests: XCTestCase {
    func testBinaryGRPCWebPublishesBeforeItsTrailerWithoutChangingForwardedBytes() async throws {
        let budget = try InspectionMemoryBudget()
        let message: [UInt8] = [0, 0, 0, 0, 2, 8, 42]
        let body = message + statusTrailer
        try await stage2WithInspectionWire(inspectionConfiguration: InspectionConfiguration(budget: budget)) { peer in
            try beginGRPCWeb(peer, length: body.count)
            try peer.origin.write(Array(message.prefix(3)))
            _ = try peer.raw.wait { stage2ResponseCount($0) == 3 }
            try peer.origin.write(Array(message.dropFirst(3)))
            let early = try await peer.inspected.wait { events in
                events.contains { event in
                    if case .grpc(.message) = event.payload, event.direction == .response {
                        true
                    } else {
                        false
                    }
                }
            }
            XCTAssertEqual(messages(in: early).map(\.bytes), [[8, 42]])
            XCTAssertTrue(release022ResponseEnds(peer.raw.events).isEmpty)
            try peer.origin.write(statusTrailer)
            let forwarded = try peer.received.wait { $0.suffix(body.count).elementsEqual(body) }
            XCTAssertEqual(Array(forwarded.suffix(body.count)), body)
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            let drain = await peer.inspector.drain()
            XCTAssertTrue(drain.drained)
            let statuses = peer.inspected.events.compactMap { event -> Int? in
                if case let .grpc(.status(status)) = event.payload, event.direction == .response {
                    status.code
                } else {
                    nil
                }
            }
            XCTAssertEqual(statuses, [0])
            XCTAssertEqual(messages(in: peer.inspected.events).map(\.bytes), [[8, 42]])
            XCTAssertTrue(diagnostics(in: peer.inspected.events).isEmpty)
            XCTAssertTrue(release022ResponseBodyBytes(peer.raw.events).isEmpty)
            XCTAssertTrue(release022Errors(peer.raw.events).isEmpty)
        }
        XCTAssertEqual(budget.retainedBytes, 0)
    }

    private var statusTrailer: [UInt8] {
        let text = Array("grpc-status: 0\r\n".utf8)
        return [0x80, 0, 0, 0, UInt8(text.count)] + text
    }

    private func beginGRPCWeb(_ peer: Stage2WireContext, length: Int, encoding: String? = nil) throws {
        let request = "POST http://127.0.0.1:\(peer.origin.port)/example.Service/Call HTTP/1.1\r\n"
            + "Host: localhost\r\nContent-Type: application/grpc-web+proto\r\nContent-Length: 0\r\n\r\n"
        try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
        _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
        let coding = encoding.map { "grpc-encoding: \($0)\r\n" } ?? ""
        let response = "HTTP/1.1 200 OK\r\nContent-Type: application/grpc-web+proto\r\n"
            + coding + "Content-Length: \(length)\r\n\r\n"
        try peer.origin.write(Array(response.utf8))
    }

    private func messages(in events: [InspectionEvent]) -> [GRPCMessage] {
        events.compactMap { event in
            if case let .grpc(.message(message)) = event.payload, event.direction == .response {
                message
            } else {
                nil
            }
        }
    }

    private func diagnostics(in events: [InspectionEvent]) -> [GRPCDiagnostic] {
        events.compactMap { event in
            if case let .grpc(.diagnostic(diagnostic)) = event.payload, event.direction == .response {
                diagnostic
            } else {
                nil
            }
        }
    }
}

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

    func testMalformedGRPCWebStopsOnlyItsObservationAndNextExchangeStillStreams() async throws {
        let validMessage: [UInt8] = [0, 0, 0, 0, 2, 8, 42]
        let badTrailerText = Array("grpc-status 0\r\n".utf8)
        let badTrailer = [UInt8(0x80), 0, 0, 0, UInt8(badTrailerText.count)] + badTrailerText
        let cases: [(name: String, body: [UInt8])] = [
            ("reserved flag", [2, 0, 0, 0, 0] + validMessage + statusTrailer),
            ("truncated message", [0, 0, 0, 0, 3, 8, 42]),
            ("malformed trailer", badTrailer + validMessage),
            ("message after trailer", statusTrailer + validMessage),
            ("encoded limit plus one", [0, 0, 0, 0, 65] + [UInt8](repeating: 0x61, count: 65) + statusTrailer)
        ]
        for example in cases {
            let events = try await observeGRPCWeb(example.body)
            XCTAssertTrue(messages(in: events).isEmpty, example.name)
            XCTAssertEqual(diagnostics(in: events).count, 1, example.name)
        }
    }

    func testCompressedGRPCMessageLimitAppliesToDecodedBytesWithoutTruncatingTransport() async throws {
        let directory = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_COMPRESSION_VECTORS"])
        let root = URL(fileURLWithPath: directory)
        for size in [64, 65] {
            let frame = [UInt8](try Data(contentsOf: root.appendingPathComponent("semantic-grpc-\(size).bin")))
            XCTAssertLessThan(frame.count - 5, 64)
            let events = try await observeGRPCWeb(frame + statusTrailer, encoding: "gzip")
            if size == 64 {
                XCTAssertEqual(messages(in: events).map(\.bytes), [[UInt8](repeating: 0x61, count: 64)])
                XCTAssertEqual(messages(in: events).map(\.compressed), [true])
                XCTAssertTrue(diagnostics(in: events).isEmpty)
            } else {
                XCTAssertTrue(messages(in: events).isEmpty)
                let failures = diagnostics(in: events)
                XCTAssertEqual(failures.count, 1)
                guard let failure = failures.first, case .limitExceeded = failure else {
                    XCTFail("Decoded message larger than the configured limit needs a limit diagnostic")
                    continue
                }
            }
        }
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

    private func observeGRPCWeb(_ body: [UInt8], encoding: String? = nil) async throws -> [InspectionEvent] {
        let budget = try InspectionMemoryBudget()
        let configuration = try InspectionConfiguration(budget: budget, maximumMessageBytes: 64)
        var observed: [InspectionEvent] = []
        try await stage2WithInspectionWire(inspectionConfiguration: configuration) { peer in
            try beginGRPCWeb(peer, length: body.count, encoding: encoding)
            try peer.origin.write(body)
            let forwarded = try peer.received.wait { $0.suffix(body.count).elementsEqual(body) }
            XCTAssertEqual(Array(forwarded.suffix(body.count)), body)
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            let drain = await peer.inspector.drain()
            XCTAssertTrue(drain.drained)
            observed = peer.inspected.events.filter { $0.direction == .response }
            XCTAssertEqual(stage2ResponseCount(peer.raw.events), body.count)
            XCTAssertTrue(release022ResponseBodyBytes(peer.raw.events).isEmpty)
            try await assertHealthyNextExchange(peer)
            XCTAssertTrue(release022Errors(peer.raw.events).isEmpty)
        }
        XCTAssertEqual(budget.retainedBytes, 0)
        return observed
    }

    private func assertHealthyNextExchange(_ peer: Stage2WireContext) async throws {
        let request = "GET http://127.0.0.1:\(peer.origin.port)/healthy HTTP/1.1\r\nHost: localhost\r\n\r\n"
        try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
        let next = try peer.origin.waitForConnection(at: 1)
        _ = try next.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
        let body = Array("data: healthy\n\n".utf8)
        let response = Array((
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                + "Content-Length: \(body.count)\r\n\r\n"
        ).utf8) + body
        try phase4BoundedWait(next.channel.writeAndFlush(ByteBuffer(bytes: response)))
        _ = try peer.received.wait { $0.suffix(body.count).elementsEqual(body) }
        _ = try peer.raw.wait { release022ResponseEnds($0).count == 2 }
        _ = try await peer.inspected.wait { events in
            events.contains { event in
                if case let .sse(message) = event.payload, event.direction == .response {
                    message.data == "healthy"
                } else {
                    false
                }
            }
        }
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

import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
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

    func testMalformedCodingEndsObservationWithoutChangingForwardedBody() async throws {
        try await stage2WithInspectionWire { peer in
            let request = "GET http://127.0.0.1:\(peer.origin.port)/malformed HTTP/1.1\r\nHost: localhost\r\n\r\n"
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
            _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let body: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0xFF, 0x00]
            try peer.origin.write(Array(
                "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: \(body.count)\r\n\r\n"
.utf8
            ) + body)
            let received = try peer.received.wait { $0.suffix(body.count).elementsEqual(body) }
            XCTAssertEqual(Array(received.suffix(body.count)), body)
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            let events = try await peer.inspected.wait { events in
                events.contains { event in
                    if case .diagnostic(.malformedCoding) = event.payload {
                        true
                    } else {
                        false
                    }
                }
            }
            XCTAssertEqual(
                events.filter { event in
                if case .diagnostic(.malformedCoding) = event.payload {
                    true
                } else {
                    false
                }
            }
.count,
                1
            )
            XCTAssertTrue(release022Errors(peer.raw.events).isEmpty)
            XCTAssertEqual(stage2ResponseCount(peer.raw.events), body.count)
        }
    }

    func testGRPCWebTextPublishesPaddedSegmentsBeforeTrailerAndEOF() async throws {
        try await stage2WithInspectionWire { peer in
            let request = "POST http://127.0.0.1:\(peer.origin.port)/example.Service/Call HTTP/1.1\r\n"
                + "Host: localhost\r\nContent-Type: application/grpc-web-text+proto\r\nContent-Length: 0\r\n\r\n"
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
            _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try peer.origin.write(Array(
                "HTTP/1.1 200 OK\r\nContent-Type: application/grpc-web-text+proto\r\nTransfer-Encoding: chunked\r\n\r\n"
.utf8
            ))
            let message: [UInt8] = [0, 0, 0, 0, 2, 8, 42]
            let encoded = Array(Data(message).base64EncodedString().utf8)
            try stage2WriteChunk(Array(encoded.prefix(3)), origin: peer.origin)
            _ = try peer.raw.wait { stage2ResponseCount($0) >= 3 }
            try stage2WriteChunk(Array(encoded.dropFirst(3)), origin: peer.origin)
            let early = try await peer.inspected.wait { events in
                events.contains { event in
                    if case .grpc(.message) = event.payload, event.direction == .response {
                        true
                    } else {
                        false
                    }
                }
            }
            assertEarlyGRPC(early, raw: peer.raw.events)
            let trailerText = Array("grpc-status: 0\r\n".utf8)
            let trailer = [UInt8(0x80), 0, 0, 0, UInt8(trailerText.count)] + trailerText
            try stage2WriteChunk(Array(Data(trailer).base64EncodedString().utf8), origin: peer.origin)
            try peer.origin.write(Array("0\r\n\r\n".utf8))
            let finished = try await peer.inspected.wait { events in
                events.contains { event in
                    if case let .grpc(.status(status)) = event.payload, event.direction == .response {
                        status.code == 0
                    } else {
                        false
                    }
                }
            }
            XCTAssertTrue(release022ResponseBodyBytes(peer.raw.events).isEmpty)
            XCTAssertEqual(
                finished.filter { event in
                if case .grpc(.message) = event.payload, event.direction == .response {
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

    func testFragmentedDeflateWebSocketMessageDoesNotDependOnFrameRetention() async throws {
        try await stage2WithInspectionWire { peer in
            try openFragmentedWebSocket(peer)
            // RFC7692 section7.2.3.1 Hello vector, fragmented around an interleaved ping.
            let frames: [UInt8] = [0x41, 3, 0xF2, 0x48, 0xCD, 0x89, 0, 0x80, 4, 0xC9, 0xC9, 0x07, 0x00]
            try peer.origin.write(frames)
            let received = try peer.received.wait { $0.suffix(frames.count).elementsEqual(frames) }
            XCTAssertEqual(Array(received.suffix(frames.count)), frames)
            let events = try await peer.inspected.wait { events in
                events.contains {
                    if case .webSocketMessage = $0.payload {
                        true
                    } else {
                        false
                    }
                }
            }
            let bytes = events.flatMap { event -> [UInt8] in
                if case let .webSocketChunk(bytes) = event.payload {
                    bytes
                } else {
                    []
                }
            }
            XCTAssertEqual(bytes, Array("Hello".utf8))
            let summary = try XCTUnwrap(events.compactMap { event -> WebSocketMessageSummary? in
                if case let .webSocketMessage(summary) = event.payload {
                    summary
                } else {
                    nil
                }
            }
.first)
            XCTAssertTrue(summary.compressed)
            XCTAssertEqual(summary.encodedBytes, 7)
            XCTAssertEqual(summary.decodedBytes, 5)
            let framesCaptured = peer.raw.events.compactMap { event -> CapturedWebSocketFrame? in
                if case let .webSocketFrame(frame) = event {
                    frame
                } else {
                    nil
                }
            }
            XCTAssertEqual(framesCaptured.map(\.byteCount), [3, 0, 4])
            XCTAssertTrue(framesCaptured.allSatisfy(\.bytes.isEmpty))
        }
    }

    func testWebSocketContextTakeoverKeepsTheDictionaryAcrossMessages() async throws {
        let directory = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_COMPRESSION_VECTORS"])
        let base = URL(fileURLWithPath: directory)
        let frames = [UInt8](try Data(contentsOf: base.appendingPathComponent("websocket-context.bin")))
        let expected = [UInt8](try Data(contentsOf: base.appendingPathComponent("websocket-context-plain.bin")))
        try await stage2WithInspectionWire { peer in
            try openContextWebSocket(peer)
            try peer.origin.write(frames)
            let forwarded = try peer.received.wait { $0.suffix(frames.count).elementsEqual(frames) }
            XCTAssertEqual(Array(forwarded.suffix(frames.count)), frames)
            let observed = try await peer.inspected.wait { events in
                events.filter {
                    if case .webSocketMessage = $0.payload {
                        true
                    } else {
                        false
                    }
                }
.count == 2
            }
            let bytes = observed.flatMap { event -> [UInt8] in
                if case let .webSocketChunk(bytes) = event.payload {
                    bytes
                } else {
                    []
                }
            }
            XCTAssertEqual(bytes, expected)
            XCTAssertFalse(observed.contains {
                if case .diagnostic = $0.payload {
                    true
                } else {
                    false
                }
            })
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

    private func assertEarlyGRPC(_ early: [InspectionEvent], raw: [CaptureEvent]) {
        let messages = early.compactMap { event -> GRPCMessage? in
            if case let .grpc(.message(message)) = event.payload, event.direction == .response {
                message
            } else {
                nil
            }
        }
        XCTAssertEqual(messages.map(\.bytes), [[8, 42]])
        XCTAssertTrue(release022ResponseEnds(raw).isEmpty)
    }

    private func openFragmentedWebSocket(_ peer: Stage2WireContext) throws {
        let request = "GET http://127.0.0.1:\(peer.origin.port)/socket HTTP/1.1\r\nHost: localhost\r\n"
            + "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
            + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            + "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover\r\n\r\n"
        try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
        _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
        try peer.origin.write(Array((
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
            + "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"
            + "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover\r\n\r\n"
        ).utf8))
        _ = try peer.raw.wait { $0.contains {
            if case .webSocketOpen = $0 {
                true
            } else {
                false
            }
        } }
    }

    private func openContextWebSocket(_ peer: Stage2WireContext) throws {
        let request = "GET http://127.0.0.1:\(peer.origin.port)/context HTTP/1.1\r\nHost: localhost\r\n"
            + "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
            + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n"
        try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
        _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
        try peer.origin.write(Array((
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
            + "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"
            + "Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n"
        ).utf8))
        _ = try peer.raw.wait { $0.contains {
            if case .webSocketOpen = $0 {
                true
            } else {
                false
            }
        } }
    }
}

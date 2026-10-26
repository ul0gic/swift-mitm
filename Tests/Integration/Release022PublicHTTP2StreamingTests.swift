import Foundation
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicHTTP2StreamingTests: XCTestCase {
    func testGRPCUnaryPreservesEnvelopeBinaryMetadataAndTrailers() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 3, 1, 2, 3]],
            responseMessages: [[0, 0, 0, 0, 2, 4, 5]]
        )
    }

    func testGRPCClientStreamingPreservesEveryEnvelope() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 1, 1], [0, 0, 0, 0, 1, 2]],
            responseMessages: [[0, 0, 0, 0, 1, 3]]
        )
    }

    func testGRPCServerStreamingPreservesCompressedRawPayload() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 1, 1]],
            responseMessages: [[1, 0, 0, 0, 2, 0xCA, 0xFE], [1, 0, 0, 0, 1, 0xFF]]
        )
    }

    func testGRPCBidirectionalStreamingReleasesResponsesBeforeRequestEnd() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 1, 1], [0, 0, 0, 0, 1, 2]],
            responseMessages: [[0, 0, 0, 0, 1, 3], [0, 0, 0, 0, 1, 4]],
            interleave: true
        )
    }

    func testGRPCTrailersOnlyFailureHasNoFabricatedBody() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(headers: self.requestHeaders(port: origin.port), endStream: true)))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([
                (":status", "200"),
                ("content-type", "application/grpc"),
                ("grpc-status", "7"),
                ("grpc-message", "permission%20denied")
            ]), endStream: true)))
            try client.wait { $0.contains(where: release022IsTerminalHeader) }
            _ = try sink.wait { $0.contains(where: release022IsResponseEnd) }
            let heads = sink.events.compactMap { event -> CapturedResponseHead? in
                if case let .responseHead(head) = event {
                    head
                } else {
                    nil
                }
            }
            XCTAssertEqual(heads.first?.headers.first { $0.name == "grpc-status" }?.value, "7")
            XCTAssertFalse(sink.events.contains {
                if case .responseBodyChunk = $0 {
                    true
                } else {
                    false
                }
            })
            XCTAssertFalse(sink.events.contains {
                if case .streamError = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    func testCancelledStreamDoesNotTerminateConcurrentGRPCControl() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let cancelled = try self.newStream(multiplexer)
            try cancelled.write(.headers(.init(headers: self.requestHeaders(port: origin.port, path: "/rpc/cancel"))))
            let first = try origin.stream(0)
            try first.wait { !$0.isEmpty }
            let control = try self.newStream(multiplexer)
            try control.write(.headers(.init(
                headers: self.requestHeaders(port: origin.port, path: "/rpc/control"),
                endStream: true
            )))
            let second = try origin.stream(1)
            try second.wait { !$0.isEmpty }
            try cancelled.write(.rstStream(.cancel))
            try second.write(.headers(.init(headers: HPACKHeaders([
                (":status", "200"),
                ("content-type", "application/grpc")
            ]))))
            let bytes: [UInt8] = [0, 0, 0, 0, 1, 42]
            try second.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: bytes)))))
            try second.write(.headers(.init(headers: HPACKHeaders([("grpc-status", "0")]), endStream: true)))
            try control.wait { $0.contains(where: release022IsTerminalHeader) }
            XCTAssertEqual(self.dataBytes(control.frames), bytes)
            _ = try sink.wait { events in events.contains {
                if case .responseEnd = $0 {
                    true
                } else {
                    false
                }
            } }
            try self.assertControlStreamHasNoError(sink)
        }
    }

    func testLateWindowUpdateAfterResponseEndKeepsRequestLegOpen() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(headers: self.requestHeaders(port: origin.port))))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([(":status", "200")]))))
            try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(string: "done")), endStream: true)))
            try client.wait { $0.contains(where: release022IsTerminalData) }
            _ = try sink.wait { $0.contains(where: release022IsResponseEnd) }
            try client.write(.windowUpdate(windowSizeIncrement: 1024))
            try client.write(.data(.init(data: .byteBuffer(ByteBuffer(string: "still-open")), endStream: true)))
            try upstream.wait { self.dataBytes($0) == Array("still-open".utf8) }
            let events = try sink.wait { $0.contains(where: release022IsRequestEnd) }
            XCTAssertFalse(events.contains {
                if case .streamError = $0 {
                    true
                } else {
                    false
                }
            })
            XCTAssertFalse(upstream.frames.contains {
                if case .windowUpdate = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    func testHTTP2SSEChunksArriveBeforeNextBarrierAndEndStream() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(
                headers: self.requestHeaders(port: origin.port, path: "/events"),
                endStream: true
            )))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([
                (":status", "200"),
                ("content-type", "text/event-stream")
            ]))))
            let chunks: [[UInt8]] = [Array(": heartbeat\n\ndata: ".utf8) + [0xE2], [0x82, 0xAC, 10, 10]]
            var released: [UInt8] = []
            for chunk in chunks {
                try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: chunk)))))
                released += chunk
                let expected = released
                try client.wait { self.dataBytes($0) == expected }
                _ = try sink.wait { self.responseBytes($0) == expected }
                XCTAssertFalse(sink.events.contains {
                    if case .responseEnd = $0 {
                        true
                    } else {
                        false
                    }
                })
            }
            try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer()), endStream: true)))
            _ = try sink.wait { $0.contains(where: release022IsResponseEnd) }
        }
    }
}

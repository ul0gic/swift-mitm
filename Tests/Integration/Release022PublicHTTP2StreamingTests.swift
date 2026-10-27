import Foundation
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicHTTP2StreamingTests: XCTestCase {
    func testGRPCBidirectionalStreamingReleasesResponsesBeforeRequestEnd() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 1, 1], [0, 0, 0, 0, 1, 2]],
            responseMessages: [[0, 0, 0, 0, 1, 3], [0, 0, 0, 0, 1, 4]],
            interleave: true
        )
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

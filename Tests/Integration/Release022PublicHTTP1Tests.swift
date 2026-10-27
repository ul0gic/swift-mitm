import NIOCore
import XCTest

import SwiftMITM

final class Release022PublicHTTP1Tests: XCTestCase {
    func testSSEReachesClientAndCaptureBeforeNextReleaseAndEOF() async throws {
        try await release022WithWire(captureLimit: 8) { origin, client, received, sink in
            let request = "GET http://127.0.0.1:\(origin.port)/events HTTP/1.1\r\nHost: localhost\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            _ = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let response = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                + "Transfer-Encoding: chunked\r\n\r\n"
            try origin.write(Array(response.utf8))
            let chunks: [[UInt8]] = [
                Array(": heartbeat\n\ndata: ".utf8) + [0xE2],
                [0x82, 0xAC] + Array("\n\ndata: second\n\n".utf8)
            ]
            var body: [UInt8] = []
            for chunk in chunks {
                try origin.write(Array("\(String(chunk.count, radix: 16))\r\n".utf8) + chunk + [13, 10])
                body += chunk
                let expectedBody = body
                _ = try received.wait { bytes in
                    bytes.count >= chunk.count && (0 ... bytes.count - chunk.count).contains {
                        Array(bytes[$0 ..< $0 + chunk.count]) == chunk
                    }
                }
                _ = try sink.wait { events in
                    events.reduce(0) { total, event in
                        if case let .responseBodyChunk(_, _, count) = event {
                            total + count
                        } else {
                            total
                        }
                    } == expectedBody.count
                }
                XCTAssertEqual(release022ResponseBodyBytes(sink.events), Array(expectedBody.prefix(8)))
                XCTAssertTrue(release022ResponseEnds(sink.events).isEmpty)
            }
            try origin.write(Array("0\r\n\r\n".utf8))
            _ = try sink.wait { release022ResponseEnds($0).count == 1 }
            XCTAssertEqual(release022ResponseBodyBytes(sink.events), Array(body.prefix(8)))
        }
    }

}

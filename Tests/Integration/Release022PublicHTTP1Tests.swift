import NIOCore
import XCTest

import SwiftMITM

final class Release022PublicHTTP1Tests: XCTestCase {
    func testObsTextKeepsNextKeepAliveExchangeCorrelated() async throws {
        try await release022WithWire(transparent: true) { origin, client, received, sink in
            let first = Array("GET /one HTTP/1.1\r\nHost: localhost\r\nX-Octet: ".utf8)
                + [0xFF, 0x80] + Array("\r\n\r\n".utf8)
            let second = Array("GET /two HTTP/1.1\r\nHost: localhost\r\n\r\n".utf8)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: first + second)))
            XCTAssertEqual(try origin.received.waitForBytes(first.count + second.count), first + second)
            let responses = Array("HTTP/1.1 200 OK\r\nContent-Length: 1\r\nX-Octet: ".utf8)
                + [0xFE] + Array("\r\n\r\naHTTP/1.1 204 No Content\r\n\r\n".utf8)
            try origin.write(responses)
            XCTAssertEqual(try received.waitForBytes(responses.count), responses)
            let events = try sink.wait { release022ResponseEnds($0).count == 2 }
            let heads = release022Requests(events)
            XCTAssertEqual(heads.map(\.path), ["/one", "/two"])
            XCTAssertEqual(heads.first?.headers.first { $0.name == "X-Octet" }?.value, "\u{00FF}\u{0080}")
            XCTAssertEqual(release022ResponseEnds(events), heads.map(\.id))
            XCTAssertTrue(release022Errors(events).isEmpty)
        }
    }

    func testCloseDelimitedResponseEndsOnlyAtCleanEOF() async throws {
        try await release022WithWire(transparent: true) { origin, client, received, sink in
            let request = Array("GET /eof HTTP/1.1\r\nHost: localhost\r\n\r\n".utf8)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: request)))
            _ = try origin.received.waitForBytes(request.count)
            let response = Array("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabc".utf8)
            try origin.write(response)
            _ = try received.waitForBytes(response.count)
            _ = try sink.wait { release022ResponseBodyBytes($0) == Array("abc".utf8) }
            XCTAssertTrue(release022ResponseEnds(sink.events).isEmpty)
            try origin.closeOutput()
            let events = try sink.wait { release022ResponseEnds($0).count == 1 }
            XCTAssertEqual(release022ResponseEnds(events), release022Requests(events).map(\.id))
            XCTAssertTrue(release022Errors(events).isEmpty)
        }
    }

    func testIncompleteFixedLengthResponseFailsExactlyOnce() async throws {
        try await release022WithWire(transparent: true) { origin, client, received, sink in
            let request = Array("GET /short HTTP/1.1\r\nHost: localhost\r\n\r\n".utf8)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: request)))
            _ = try origin.received.waitForBytes(request.count)
            let response = Array("HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nabc".utf8)
            try origin.write(response)
            _ = try received.waitForBytes(response.count)
            try origin.closeOutput()
            let events = try sink.wait { release022Errors($0).count == 1 }
            try received.waitForClose()
            XCTAssertEqual(release022Errors(events), release022Requests(events).map(\.id))
            XCTAssertTrue(release022ResponseEnds(events).isEmpty)
        }
    }

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

    func testRepeatedConnectionUpgradeWaitsForBothClosesAndSuppressesLaterFrames() async throws {
        try await release022WithWire(transparent: true) { origin, client, received, sink in
            let request = Array((
                "GET /socket HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
                    + "Connection: keep-alive\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
                    + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
            ).utf8)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: request)))
            _ = try origin.received.waitForBytes(request.count)
            let response = Array((
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: keep-alive\r\n"
                    + "Connection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
            ).utf8)
            try origin.write(response)
            _ = try received.waitForBytes(response.count)
            _ = try sink.wait { $0.contains {
                if case .webSocketOpen = $0 {
                    true
                } else {
                    false
                }
            } }
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: WebSocketWire.clientFrames)))
            _ = try origin.received.waitForBytes(request.count + WebSocketWire.clientFrames.count)
            _ = try sink.wait { $0.filter {
                if case .webSocketFrame = $0 {
                    true
                } else {
                    false
                }
            }
.count == 2
            }
            XCTAssertFalse(sink.events.contains {
                if case .webSocketClose = $0 {
                    true
                } else {
                    false
                }
            })
            try origin.write(WebSocketWire.serverFrames + WebSocketWire.serverBinaryFrame)
            let byteCount = response.count + WebSocketWire.serverFrames.count + WebSocketWire.serverBinaryFrame.count
            _ = try received.waitForBytes(byteCount)
            _ = try sink.wait { $0.contains {
                if case .webSocketClose = $0 {
                    true
                } else {
                    false
                }
            } }
            let events = sink.events
            XCTAssertEqual(
                events.filter {
                if case .webSocketClose = $0 {
                    true
                } else {
                    false
                }
            }
.count,
                1
            )
            let terminal = try XCTUnwrap(events.firstIndex {
                if case .webSocketClose = $0 {
                    true
                } else {
                    false
                }
            })
            XCTAssertEqual(
                events[..<terminal]
.filter {
                if case .webSocketFrame = $0 {
                    true
                } else {
                    false
                }
            }
.count,
                4
            )
            XCTAssertFalse(events.suffix(from: terminal + 1).contains {
                if case .webSocketFrame = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    func testIPv6CaptureAuthoritiesRemainBracketed() async throws {
        try await release022WithWire(transparent: true, ipv6: true) { origin, client, _, sink in
            let authority = "[::1]:\(origin.port)"
            let request = Array("GET /ipv6 HTTP/1.1\r\nHost: \(authority)\r\n\r\n".utf8)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: request)))
            XCTAssertEqual(try origin.received.waitForBytes(request.count), request)
            try origin.write(Array("HTTP/1.1 204 No Content\r\n\r\n".utf8))
            let events = try sink.wait { release022ResponseEnds($0).count == 1 }
            XCTAssertEqual(release022Requests(events).first?.authority, authority)
            XCTAssertEqual(release022Requests(events).first?.target?.logicalAuthority, authority)
        }
    }

    func testObservationParserFailureTerminatesEveryOutstandingCaptureOnce() async throws {
        var observed: Release022Events?
        try await release022WithWire(transparent: true) { origin, client, _, sink in
            observed = sink
            let requests = Array((
                "GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n"
                    + "GET /two HTTP/1.1\r\nHost: localhost\r\n\r\n"
            ).utf8)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: requests)))
            _ = try origin.received.waitForBytes(requests.count)
            try origin.write(Array("HTTP/1.1 200 OK\r\nNot-A-Field\r\n\r\n".utf8))
            _ = try sink.wait { release022Errors($0).count == 2 }
        }
        let events = try XCTUnwrap(observed).events
        let ids = release022Requests(events).map(\.id)
        XCTAssertEqual(Set(release022Errors(events)), Set(ids))
        XCTAssertEqual(release022Errors(events).count, 2)
        XCTAssertTrue(release022ResponseEnds(events).isEmpty)
    }
}

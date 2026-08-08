import Foundation
import XCTest

@testable import SwiftMITM

final class WebSocketFrameDecoderTests: XCTestCase {
    private func decode(_ feeds: [[UInt8]], captureLimit: Int = 0) -> [WebSocketFrameDecoder.Frame] {
        let decoder = WebSocketFrameDecoder(captureLimit: captureLimit)
        var frames: [WebSocketFrameDecoder.Frame] = []
        for feed in feeds {
            decoder.decode(feed) { frames.append($0) }
        }
        return frames
    }

    /// Server->client text frame: FIN, opcode 0x1, unmasked, payload "hi".
    func testUnmaskedTextFrame() {
        let frames = decode([[0x81, 0x02, 0x68, 0x69]])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.opcode, .text)
        XCTAssertTrue(frames.first?.fin ?? false)
        XCTAssertEqual(frames.first.map { String(bytes: $0.bytes, encoding: .utf8) }, "hi")
        XCTAssertEqual(frames.first?.byteCount, 2)
        XCTAssertFalse(frames.first?.truncated ?? true)
    }

    /// Client->server frames are masked; the decoder must unmask with the 4-byte key.
    func testMaskedFrameIsUnmasked() {
        let key: [UInt8] = [0x37, 0xFA, 0x21, 0x3D]
        let payload: [UInt8] = Array("hi".utf8)
        let masked = payload.enumerated().map { $0.element ^ key[$0.offset & 3] }
        var frame: [UInt8] = [0x81, 0x82]
        frame.append(contentsOf: key)
        frame.append(contentsOf: masked)
        let frames = decode([frame])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first.map { String(bytes: $0.bytes, encoding: .utf8) }, "hi")
    }

    /// A frame split across two reads must still decode as one frame with the payload intact.
    func testFrameSpanningMultipleReads() {
        let frames = decode([[0x82, 0x04, 0xDE], [0xAD, 0xBE], [0xEF]])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.opcode, .binary)
        XCTAssertEqual(frames.first?.bytes, [0xDE, 0xAD, 0xBE, 0xEF])
    }

    /// Two frames in one read decode independently and in order.
    func testTwoFramesInOneRead() {
        let frames = decode([[0x81, 0x01, 0x41, 0x81, 0x01, 0x42]])
        XCTAssertEqual(frames.count, 2)
        XCTAssertEqual(frames.map { String(bytes: $0.bytes, encoding: .utf8) }, ["A", "B"])
    }

    /// 126 signals a 16-bit extended length; payload is 200 bytes.
    func testExtendedLength16() {
        var frame: [UInt8] = [0x82, 126, 0x00, 0xC8]
        frame.append(contentsOf: Array(repeating: 0x5A, count: 200))
        let frames = decode([frame])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.byteCount, 200)
        XCTAssertEqual(frames.first?.bytes.count, 200)
    }

    /// Payload beyond the capture limit is bounded but byteCount reports the true length and truncated is set.
    func testPayloadCappedAtCaptureLimit() {
        var frame: [UInt8] = [0x82, 126, 0x00, 0xC8]
        frame.append(contentsOf: Array(repeating: 0x5A, count: 200))
        let frames = decode([frame], captureLimit: 50)
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.byteCount, 200)
        XCTAssertEqual(frames.first?.bytes.count, 50)
        XCTAssertTrue(frames.first?.truncated ?? false)
    }

    /// RSV1 (0x40) marks a permessage-deflate-compressed message; framing is unaffected.
    func testCompressedFlag() {
        let frames = decode([[0xC1, 0x01, 0x78]])
        XCTAssertEqual(frames.count, 1)
        XCTAssertTrue(frames.first?.compressed ?? false)
        XCTAssertTrue(frames.first?.fin ?? false)
    }

    /// A 101 handshake response followed by a frame in the same buffer: the head parses, `.upgraded` fires,
    /// and the trailing frame bytes route to the tunnel (never mis-parsed as a second HTTP head).
    func testParserRoutesPostHandshakeBytesToTunnel() {
        let parser = HTTP1MessageParser(mode: .response)
        var outputs: [HTTP1ParserOutput] = []
        var tunnel: [UInt8] = []
        let handshake = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
        var bytes = Array(handshake.utf8)
        bytes.append(contentsOf: [0x81, 0x02, 0x68, 0x69]) // "hi" frame appended to the same read

        parser.feed(
            bytes,
            methodProvider: { "GET" },
            emit: { outputs.append($0) },
            tunnelBytes: { tunnel.append(contentsOf: $0) }
        )

        XCTAssertTrue(outputs.contains(.upgraded))
        XCTAssertEqual(tunnel, [0x81, 0x02, 0x68, 0x69])

        let decoder = WebSocketFrameDecoder(captureLimit: 0)
        var frames: [WebSocketFrameDecoder.Frame] = []
        decoder.decode(tunnel) { frames.append($0) }
        XCTAssertEqual(frames.first.map { String(bytes: $0.bytes, encoding: .utf8) }, "hi")
    }

    /// Close frame carries a 2-byte big-endian status code and an optional UTF-8 reason.
    func testCloseFrameCodeAndReason() {
        var frame: [UInt8] = [0x88, 0x05, 0x03, 0xE8]
        frame.append(contentsOf: Array("bye".utf8))
        let frames = decode([frame])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.opcode, .connectionClose)
        XCTAssertEqual(frames.first?.closeCode, 1000)
        XCTAssertEqual(frames.first?.closeReason, "bye")
    }
}

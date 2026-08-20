import Foundation
import XCTest

@testable import SwiftMITM

final class WebSocketFrameDecoderTests: XCTestCase {
    private func decode(_ feeds: [[UInt8]], captureLimit: Int = 1_048_576) -> [WebSocketFrameDecoder.Frame] {
        let decoder = WebSocketFrameDecoder(captureLimit: captureLimit)
        var frames: [WebSocketFrameDecoder.Frame] = []
        for feed in feeds {
            decoder.decode(feed) { frames.append($0) }
        }
        return frames
    }

    func testUnmaskedTextFrame() {
        let frames = decode([[0x81, 0x02, 0x68, 0x69]])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.opcode, .text)
        XCTAssertTrue(frames.first?.fin ?? false)
        XCTAssertEqual(frames.first.map { String(bytes: $0.bytes, encoding: .utf8) }, "hi")
        XCTAssertEqual(frames.first?.byteCount, 2)
        XCTAssertFalse(frames.first?.truncated ?? true)
    }

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

    func testFrameSpanningMultipleReads() {
        let frames = decode([[0x82, 0x04, 0xDE], [0xAD, 0xBE], [0xEF]])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.opcode, .binary)
        XCTAssertEqual(frames.first?.bytes, [0xDE, 0xAD, 0xBE, 0xEF])
    }

    func testTwoFramesInOneRead() {
        let frames = decode([[0x81, 0x01, 0x41, 0x81, 0x01, 0x42]])
        XCTAssertEqual(frames.count, 2)
        XCTAssertEqual(frames.map { String(bytes: $0.bytes, encoding: .utf8) }, ["A", "B"])
    }

    func testExtendedLength16() {
        var frame: [UInt8] = [0x82, 126, 0x00, 0xC8]
        frame.append(contentsOf: Array(repeating: 0x5A, count: 200))
        let frames = decode([frame])
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.byteCount, 200)
        XCTAssertEqual(frames.first?.bytes.count, 200)
    }

    func testExtendedLength16RejectsNonMinimalEncoding() {
        let decoder = WebSocketFrameDecoder(captureLimit: 1)
        var frames: [WebSocketFrameDecoder.Frame] = []
        decoder.decode([0x82, 126, 0x00, 0x7D]) { frames.append($0) }
        decoder.decode([0x81, 0x00]) { frames.append($0) }
        XCTAssertEqual(frames.count, 0)
    }

    func testPayloadCappedAtCaptureLimit() {
        var frame: [UInt8] = [0x82, 126, 0x00, 0xC8]
        frame.append(contentsOf: Array(repeating: 0x5A, count: 200))
        let frames = decode([frame], captureLimit: 50)
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.byteCount, 200)
        XCTAssertEqual(frames.first?.bytes.count, 50)
        XCTAssertTrue(frames.first?.truncated ?? false)
    }

    func testZeroCaptureLimitReportsObservedPayloadWithoutBytes() {
        let frames = decode([[0x82, 0x04, 0xDE, 0xAD, 0xBE, 0xEF]], captureLimit: 0)
        XCTAssertEqual(frames.first?.bytes, [])
        XCTAssertEqual(frames.first?.byteCount, 4)
        XCTAssertTrue(frames.first?.truncated ?? false)
    }

    func testNegativeCaptureLimitReportsObservedPayloadWithoutBytes() {
        let frames = decode([[0x81, 0x01, 0x41]], captureLimit: -1)
        XCTAssertEqual(frames.first?.bytes, [])
        XCTAssertEqual(frames.first?.byteCount, 1)
        XCTAssertTrue(frames.first?.truncated ?? false)
    }

    func testZeroLengthFramesEmitWithoutFollowingPayloadByte() {
        let frames = decode([[0x81, 0x00, 0x89, 0x00]], captureLimit: 0)
        XCTAssertEqual(frames.count, 2)
        XCTAssertEqual(frames.map(\.opcode), [.text, .ping])
        XCTAssertEqual(frames.map(\.byteCount), [0, 0])
        XCTAssertEqual(frames.map(\.truncated), [false, false])
    }

    func testExtendedLength64AcrossSplitHeaderAndPayload() {
        let payload = Array(repeating: UInt8(0x5A), count: 65_536)
        let frames = decode([[0x82, 127, 0, 0, 0], [0, 0, 1, 0, 0], payload], captureLimit: 1)
        XCTAssertEqual(frames.count, 1)
        XCTAssertEqual(frames.first?.bytes, [0x5A])
        XCTAssertEqual(frames.first?.byteCount, 65_536)
        XCTAssertTrue(frames.first?.truncated ?? false)
    }

    func testExtendedLength64RejectsForbiddenHighBitWithoutPayload() {
        let decoder = WebSocketFrameDecoder(captureLimit: 1)
        var frames: [WebSocketFrameDecoder.Frame] = []
        decoder.decode([0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0]) { frames.append($0) }
        decoder.decode([0x81, 0x00]) { frames.append($0) }
        XCTAssertEqual(frames.count, 0)
    }

    func testExtendedLength64RejectsValueAboveIntMaxWithoutPayload() {
        let value = UInt64(Int.max) &+ 1
        var header: [UInt8] = [0x82, 127]
        for shift in stride(from: 56, through: 0, by: -8) {
            header.append(UInt8(truncatingIfNeeded: value >> UInt64(shift)))
        }
        let decoder = WebSocketFrameDecoder(captureLimit: 1)
        var frames: [WebSocketFrameDecoder.Frame] = []
        decoder.decode(header) { frames.append($0) }
        decoder.decode([0x81, 0x00]) { frames.append($0) }
        XCTAssertEqual(frames.count, 0)
    }

    func testExtendedLength64RejectsNonMinimalEncoding() {
        let decoder = WebSocketFrameDecoder(captureLimit: 1)
        var frames: [WebSocketFrameDecoder.Frame] = []
        decoder.decode([0x82, 127, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF]) { frames.append($0) }
        decoder.decode([0x81, 0x00]) { frames.append($0) }
        XCTAssertEqual(frames.count, 0)
    }

    func testCompressedFlag() {
        let frames = decode([[0xC1, 0x02, 0x78, 0x9C]])
        XCTAssertEqual(frames.count, 1)
        XCTAssertTrue(frames.first?.compressed ?? false)
        XCTAssertTrue(frames.first?.fin ?? false)
        XCTAssertEqual(frames.first?.bytes, [0x78, 0x9C])
    }

    func testRSV2AndRSV3AreRejected() {
        XCTAssertEqual(decode([[0xA1, 0x00]]).count, 0)
        XCTAssertEqual(decode([[0x91, 0x00]]).count, 0)
    }

    func testDecodeReportsTerminalFailureAndRemainsFailed() {
        let decoder = WebSocketFrameDecoder(captureLimit: 16)
        var frames: [WebSocketFrameDecoder.Frame] = []

        XCTAssertFalse(decoder.decode([0x83, 0x00]) { frames.append($0) })
        XCTAssertFalse(decoder.decode([0x81, 0x00]) { frames.append($0) })
        XCTAssertTrue(frames.isEmpty)
    }

    func testControlFrameWithRSV1IsRejected() {
        XCTAssertEqual(decode([[0xC9, 0x00]]).count, 0)
    }

    func testContinuationFrameWithRSV1IsRejected() {
        XCTAssertEqual(decode([[0xC0, 0x00]]).count, 0)
    }

    func testFragmentedControlFrameIsRejected() {
        XCTAssertEqual(decode([[0x09, 0x00]]).count, 0)
    }

    func testExtendedControlFrameLengthIsRejectedWithoutPayload() {
        let decoder = WebSocketFrameDecoder(captureLimit: 1)
        var frames: [WebSocketFrameDecoder.Frame] = []
        decoder.decode([0x89, 126, 0x00, 0x7D]) { frames.append($0) }
        decoder.decode([0x81, 0x00]) { frames.append($0) }
        XCTAssertEqual(frames.count, 0)
    }

    func testParserRoutesPostHandshakeBytesToTunnel() {
        let parser = HTTP1MessageParser(mode: .response)
        var outputs: [HTTP1ParserOutput] = []
        var tunnel: [UInt8] = []
        let handshake = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
        var bytes = Array(handshake.utf8)
        bytes.append(contentsOf: [0x81, 0x02, 0x68, 0x69])

        parser.feed(
            bytes,
            requestProvider: {
                HTTP1RequestMetadata(method: "GET", webSocketUpgradeRequested: true)
            },
            emit: { outputs.append($0) },
            tunnelBytes: { tunnel.append(contentsOf: $0) }
        )

        XCTAssertTrue(outputs.contains(.upgraded))
        XCTAssertEqual(tunnel, [0x81, 0x02, 0x68, 0x69])

        let decoder = WebSocketFrameDecoder(captureLimit: 2)
        var frames: [WebSocketFrameDecoder.Frame] = []
        decoder.decode(tunnel) { frames.append($0) }
        XCTAssertEqual(frames.first.map { String(bytes: $0.bytes, encoding: .utf8) }, "hi")
    }

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

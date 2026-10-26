import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class HTTP1RejectedWebSocketUpgradeTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    func testRejectedUpgradeCapturesSubsequentHTTPWithoutWebSocketEvents() throws {
        let fixture = Fixture()
        defer { fixture.finish() }

        try fixture.requestChannel.writeInbound(buffer(
            "GET /socket HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
        ))
        try fixture.responseChannel.writeInbound(buffer(
            Array("HTTP/1.1 403 Forbidden\r\nContent-Length: 2\r\n\r\n".utf8) + [0x88, 0x00]
        ))
        try fixture.requestChannel.writeInbound(buffer(
            "GET /after HTTP/1.1\r\nHost: example.com\r\n\r\n"
        ))
        try fixture.responseChannel.writeInbound(buffer("HTTP/1.1 204 No Content\r\n\r\n"))

        let requestHeads = fixture.sink.events.compactMap { event -> CapturedRequestHead? in
            guard case .requestHead(let head) = event else { return nil }
            return head
        }
        let responseHeads = fixture.sink.events.compactMap { event -> CapturedResponseHead? in
            guard case .responseHead(let head) = event else { return nil }
            return head
        }
        let requestEnds = fixture.sink.events.compactMap { event -> UUID? in
            guard case .requestEnd(let requestID, _) = event else { return nil }
            return requestID
        }
        let webSocketEvents = fixture.sink.events.filter { event in
            switch event {
            case .webSocketOpen, .webSocketFrame, .webSocketClose:
                true
            default:
                false
            }
        }

        XCTAssertEqual(requestHeads.map(\.path), ["/socket", "/after"])
        XCTAssertEqual(responseHeads.map(\.status), [403, 204])
        XCTAssertEqual(requestEnds, requestHeads.map(\.id))
        XCTAssertTrue(webSocketEvents.isEmpty)
    }

    func testAcceptedUpgradePreservesWireBytesAndCaptureOrdering() throws {
        let fixture = Fixture()
        defer { fixture.finish() }
        let requestHead = Array(
            "GET /socket HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n".utf8
        )
        let responseHead = Array(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n".utf8
        )
        let clientFrame: [UInt8] = [0x81, 0x00]
        let serverFrame: [UInt8] = [0x82, 0x00]

        try fixture.requestChannel.writeInbound(buffer(requestHead))
        try fixture.responseChannel.writeInbound(buffer(responseHead + serverFrame))
        try fixture.requestChannel.writeInbound(buffer(clientFrame))

        XCTAssertEqual(readBytes(from: fixture.requestChannel), requestHead + clientFrame)
        XCTAssertEqual(readBytes(from: fixture.responseChannel), responseHead + serverFrame)
        XCTAssertEqual(
            fixture.eventKinds,
            ["requestHead", "responseHead", "webSocketOpen", "serverFrame", "clientFrame"]
        )
    }

    private final class Fixture {
        let sink = RecordingSink()
        let requestChannel: EmbeddedChannel
        let responseChannel: EmbeddedChannel

        init() {
            let correlator = HTTP1ExchangeCorrelator()
            let session = WebSocketCaptureSession()
            requestChannel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
                direction: .request,
                authority: "example.com:443",
                correlator: correlator,
                sink: sink,
                captureBodyLimit: 8,
                webSocketSession: session
            ))
            responseChannel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
                direction: .response,
                authority: "example.com:443",
                correlator: correlator,
                sink: sink,
                captureBodyLimit: 8,
                webSocketSession: session
            ))
        }

        var eventKinds: [String] {
            sink.events.compactMap { event in
                switch event {
                case .requestHead:
                    "requestHead"
                case .responseHead:
                    "responseHead"
                case .webSocketOpen:
                    "webSocketOpen"
                case .webSocketFrame(let frame):
                    frame.direction == .clientToServer ? "clientFrame" : "serverFrame"
                default:
                    nil
                }
            }
        }

        func finish() {
            _ = try? requestChannel.finish()
            _ = try? responseChannel.finish()
        }
    }

    private func buffer(_ string: String) -> ByteBuffer {
        buffer(Array(string.utf8))
    }

    private func buffer(_ bytes: [UInt8]) -> ByteBuffer {
        ByteBuffer(bytes: bytes)
    }

    private func readBytes(from channel: EmbeddedChannel) -> [UInt8] {
        var bytes: [UInt8] = []
        while let buffer = try? channel.readInbound(as: ByteBuffer.self) {
            bytes.append(contentsOf: buffer.readableBytesView)
        }
        return bytes
    }
}

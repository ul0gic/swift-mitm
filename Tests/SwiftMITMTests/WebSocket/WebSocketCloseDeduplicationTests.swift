import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class WebSocketCloseDeduplicationTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    func testBothCloseFramesAndInactivityEmitOneConnectionClose() throws {
        let correlator = HTTP1ExchangeCorrelator()
        let closeEmissionState = WebSocketCloseEmissionState()
        let sink = RecordingSink()
        let requestChannel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .request,
            authority: "example.com:443",
            correlator: correlator,
            sink: sink,
            captureBodyLimit: 8,
            closeEmissionState: closeEmissionState
        ))
        let responseChannel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .response,
            authority: "example.com:443",
            correlator: correlator,
            sink: sink,
            captureBodyLimit: 8,
            closeEmissionState: closeEmissionState
        ))

        try requestChannel.writeInbound(buffer(
            "GET /socket HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
        ))
        try responseChannel.writeInbound(buffer(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
        ))
        try requestChannel.writeInbound(buffer([0x88, 0x02, 0x03, 0xE8]))
        try responseChannel.writeInbound(buffer([0x88, 0x02, 0x03, 0xE8]))
        _ = try requestChannel.finish()
        _ = try responseChannel.finish()

        let frameDirections = sink.events.compactMap { event -> WebSocketDirection? in
            if case .webSocketFrame(let frame) = event {
                return frame.direction
            }
            return nil
        }
        let closes = sink.events.compactMap { event -> UUID? in
            if case .webSocketClose(let connectionID, _, _, _) = event {
                return connectionID
            }
            return nil
        }
        XCTAssertEqual(frameDirections, [.clientToServer, .serverToClient])
        XCTAssertEqual(closes.count, 1)
    }

    private func buffer(_ value: String) -> ByteBuffer {
        buffer(Array(value.utf8))
    }

    private func buffer(_ bytes: [UInt8]) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: bytes.count)
        buffer.writeBytes(bytes)
        return buffer
    }
}

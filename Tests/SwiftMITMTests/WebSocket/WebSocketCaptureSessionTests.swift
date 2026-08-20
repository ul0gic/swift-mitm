import Foundation
import NIOConcurrencyHelpers
import XCTest

@testable import SwiftMITM

final class WebSocketCaptureSessionTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    func testOpenAndBidirectionalFramesShareIdentityAndDirectionalDecoderState() {
        let id = UUID()
        let sink = RecordingSink()
        let session = WebSocketCaptureSession()

        session.prepare(id: id, sink: sink, captureLimit: 3)
        session.open(id: id, sink: sink, captureLimit: 3, permessageDeflate: true)
        session.capture([0x81, 0x04, 0x74], direction: .clientToServer)
        session.capture([0x65, 0x73, 0x74], direction: .clientToServer)
        session.capture([0x82, 0x02, 0xCA, 0xFE], direction: .serverToClient)

        XCTAssertEqual(sink.events.count, 3)
        guard case let .webSocketOpen(openID, _, permessageDeflate) = sink.events[0] else {
            return XCTFail("Expected WebSocket open")
        }
        XCTAssertEqual(openID, id)
        XCTAssertTrue(permessageDeflate)

        guard case let .webSocketFrame(clientFrame) = sink.events[1] else {
            return XCTFail("Expected client frame")
        }
        XCTAssertEqual(clientFrame.connectionID, id)
        XCTAssertEqual(clientFrame.direction, .clientToServer)
        XCTAssertEqual(clientFrame.opcode, .text)
        XCTAssertEqual(clientFrame.bytes, Array("tes".utf8))
        XCTAssertEqual(clientFrame.byteCount, 4)
        XCTAssertTrue(clientFrame.truncated)

        guard case let .webSocketFrame(serverFrame) = sink.events[2] else {
            return XCTFail("Expected server frame")
        }
        XCTAssertEqual(serverFrame.connectionID, id)
        XCTAssertEqual(serverFrame.direction, .serverToClient)
        XCTAssertEqual(serverFrame.opcode, .binary)
        XCTAssertEqual(serverFrame.bytes, [0xCA, 0xFE])
        XCTAssertEqual(serverFrame.byteCount, 2)
        XCTAssertFalse(serverFrame.truncated)
    }

    func testCloseFramesEmitInObservationOrderAndShareCloseDeduplication() {
        let id = UUID()
        let sink = RecordingSink()
        let session = WebSocketCaptureSession()
        session.open(id: id, sink: sink, captureLimit: 5, permessageDeflate: false)

        session.capture([0x88, 0x05, 0x03, 0xE8, 0x62, 0x79, 0x65], direction: .clientToServer)
        session.capture([0x88, 0x02, 0x03, 0xE8], direction: .serverToClient)
        session.close()

        XCTAssertEqual(sink.events.count, 4)
        guard case let .webSocketFrame(clientFrame) = sink.events[1] else {
            return XCTFail("Expected client close frame")
        }
        XCTAssertEqual(clientFrame.direction, .clientToServer)
        XCTAssertEqual(clientFrame.closeCode, 1000)
        XCTAssertEqual(clientFrame.closeReason, "bye")

        guard case let .webSocketClose(closeID, _, code, reason) = sink.events[2] else {
            return XCTFail("Expected WebSocket close")
        }
        XCTAssertEqual(closeID, id)
        XCTAssertEqual(code, 1000)
        XCTAssertEqual(reason, "bye")

        guard case let .webSocketFrame(serverFrame) = sink.events[3] else {
            return XCTFail("Expected server close frame")
        }
        XCTAssertEqual(serverFrame.direction, .serverToClient)
        XCTAssertEqual(serverFrame.closeCode, 1000)
    }

    func testCaptureReportsDecoderFailureWithoutChangingHTTP1Lifecycle() {
        let sink = RecordingSink()
        let session = WebSocketCaptureSession()
        session.open(id: UUID(), sink: sink, captureLimit: 8, permessageDeflate: false)

        XCTAssertFalse(session.capture([0x83, 0x00], direction: .clientToServer))

        XCTAssertEqual(sink.events.count, 1)
        guard case .webSocketOpen = sink.events[0] else {
            return XCTFail("Expected only WebSocket open")
        }
    }
}

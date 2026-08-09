import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class HTTP1InformationalResponseCaptureTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    func testInformationalAndFinalResponsesShareRequestWithoutEarlyEnd() throws {
        let requestID = UUID()
        let correlator = HTTP1ExchangeCorrelator()
        correlator.enqueue(id: requestID, method: "GET")
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .response,
            authority: "example.com:443",
            correlator: correlator,
            sink: sink
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(buffer("HTTP/1.1 103 Early Hints\r\nLink: </style.css>\r\n\r\n"))

        XCTAssertEqual(responseHeads(sink.events).map(\.requestID), [requestID])
        XCTAssertEqual(responseEndIDs(sink.events), [])
        XCTAssertEqual(correlator.peek()?.id, requestID)

        try channel.writeInbound(buffer("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))

        XCTAssertEqual(responseHeads(sink.events).map(\.requestID), [requestID, requestID])
        XCTAssertEqual(responseHeads(sink.events).map(\.status), [103, 200])
        XCTAssertEqual(responseEndIDs(sink.events), [requestID])
        XCTAssertNil(correlator.peek())
    }

    private func buffer(_ value: String) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: value.utf8.count)
        buffer.writeString(value)
        return buffer
    }

    private func responseHeads(_ events: [CaptureEvent]) -> [CapturedResponseHead] {
        events.compactMap {
            if case .responseHead(let head) = $0 { return head }
            return nil
        }
    }

    private func responseEndIDs(_ events: [CaptureEvent]) -> [UUID] {
        events.compactMap {
            if case .responseEnd(let requestID, _) = $0 { return requestID }
            return nil
        }
    }
}

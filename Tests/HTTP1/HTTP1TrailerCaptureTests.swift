import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class HTTP1TrailerCaptureTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    func testResponseTrailersEmitBeforeResponseEnd() throws {
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

        try channel.writeInbound(buffer(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nDigest: value\r\n\r\n"
        ))

        let ordered = sink.events.compactMap { event -> String? in
            switch event {
            case .responseHead: return "head"
            case .responseBodyChunk: return "body"
            case let .responseTrailers(id, headers):
                XCTAssertEqual(id, requestID)
                XCTAssertEqual(headers, [HTTPHeaderField(name: "Digest", value: "value")])
                return "trailers"
            case .responseEnd: return "end"
            default: return nil
            }
        }
        XCTAssertEqual(ordered, ["head", "body", "trailers", "end"])
    }

    private func buffer(_ value: String) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: value.utf8.count)
        buffer.writeString(value)
        return buffer
    }
}

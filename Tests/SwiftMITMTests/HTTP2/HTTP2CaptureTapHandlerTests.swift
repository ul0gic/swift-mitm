import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOHPACK
import NIOHTTP2
import XCTest

@testable import SwiftMITM

final class HTTP2CaptureTapHandlerTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    func testInformationalFinalBodyTrailersAndEndOrder() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channel = responseChannel(requestID: requestID, sink: sink)
        defer { _ = try? channel.finish() }

        try channel.writeInbound(responseHeaders(status: 103, endStream: false))
        try channel.writeInbound(responseHeaders(status: 200, endStream: false))
        try channel.writeInbound(data("body", endStream: false))
        try channel.writeInbound(trailers([("digest", "value")], endStream: true))

        XCTAssertEqual(eventKinds(sink.events), ["response:103", "response:200", "body", "trailers", "end"])
        XCTAssertEqual(streamErrorCount(sink.events), 0)
    }

    func testRequestInitialBodyTrailersAndEndOrder() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .request,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(requestHeaders(endStream: false))
        try channel.writeInbound(data("body", endStream: false))
        try channel.writeInbound(trailers([("digest", "value")], endStream: true))

        XCTAssertEqual(eventKinds(sink.events), ["request", "body", "trailers", "end"])
    }

    func testDataBeforeHeadsAndDuplicatePseudoHeadersShareOneError() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let errorState = HTTP2StreamErrorState()
        let request = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .request,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink,
            errorState: errorState
        ))
        let response = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .response,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink,
            errorState: errorState
        ))
        defer {
            _ = try? request.finish()
            _ = try? response.finish()
        }

        try request.writeInbound(data("x", endStream: false))
        var invalid = HPACKHeaders()
        invalid.add(name: ":status", value: "200")
        invalid.add(name: ":status", value: "204")
        try response.writeInbound(HTTP2Frame.FramePayload.headers(.init(headers: invalid, endStream: true)))

        XCTAssertEqual(streamErrorCount(sink.events), 1)
        XCTAssertEqual(streamErrorMessages(sink.events), ["HTTP/2 capture stream invalid"])
    }

    func testTrailersWithoutEndStreamEmitErrorWithoutEnd() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channel = responseChannel(requestID: requestID, sink: sink)
        defer { _ = try? channel.finish() }

        try channel.writeInbound(responseHeaders(status: 200, endStream: false))
        try channel.writeInbound(trailers([("digest", "value")], endStream: false))

        XCTAssertEqual(streamErrorCount(sink.events), 1)
        XCTAssertFalse(eventKinds(sink.events).contains("end"))
    }

    func testPrematureInactivityEmitsOneError() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channel = responseChannel(requestID: requestID, sink: sink)
        try channel.writeInbound(responseHeaders(status: 200, endStream: false))

        _ = try channel.finish()

        XCTAssertEqual(streamErrorCount(sink.events), 1)
    }

    private func responseChannel(requestID: UUID, sink: CaptureEventSink) -> EmbeddedChannel {
        EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .response,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink
        ))
    }

    private func requestHeaders(endStream: Bool) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "POST")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":authority", value: "example.com")
        headers.add(name: ":path", value: "/")
        return .headers(.init(headers: headers, endStream: endStream))
    }

    private func responseHeaders(status: Int, endStream: Bool) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: String(status))
        return .headers(.init(headers: headers, endStream: endStream))
    }

    private func trailers(_ fields: [(String, String)], endStream: Bool) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        for (name, value) in fields { headers.add(name: name, value: value) }
        return .headers(.init(headers: headers, endStream: endStream))
    }

    private func data(_ value: String, endStream: Bool) -> HTTP2Frame.FramePayload {
        var buffer = ByteBufferAllocator().buffer(capacity: value.utf8.count)
        buffer.writeString(value)
        return .data(.init(data: .byteBuffer(buffer), endStream: endStream))
    }

    private func eventKinds(_ events: [CaptureEvent]) -> [String] {
        events.compactMap { event in
            switch event {
            case .requestHead: return "request"
            case .responseHead(let head): return "response:\(head.status)"
            case .requestBodyChunk, .responseBodyChunk: return "body"
            case .requestTrailers, .responseTrailers: return "trailers"
            case .requestEnd, .responseEnd: return "end"
            default: return nil
            }
        }
    }

    private func streamErrorCount(_ events: [CaptureEvent]) -> Int {
        streamErrorMessages(events).count
    }

    private func streamErrorMessages(_ events: [CaptureEvent]) -> [String] {
        events.compactMap { event in
            if case .streamError(_, let message) = event {
                return message
            }
            return nil
        }
    }
}

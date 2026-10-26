import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOHPACK
import NIOHTTP2
import XCTest

@testable import SwiftMITM

final class HTTP2WebSocketStreamIntegrationTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    private struct StreamChannels {
        let request: EmbeddedChannel
        let response: EmbeddedChannel
    }

    private enum ExpectedError: Error {
        case teardown
    }

    func testAcceptedHandshakeCapturesFragmentedBidirectionalFramesAndClosesAfterBothEnds() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channels = makeChannels(requestID: requestID, sink: sink, captureLimit: 64)
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(
            status: 103,
            endStream: false
        ))
        try channels.response.writeInbound(responseHeaders(
            status: 200,
            endStream: false,
            fields: [("sec-websocket-extensions", "permessage-deflate")]
        ))
        try channels.request.writeInbound(data([0x81, 0x04, 0x74], endStream: false))
        try channels.request.writeInbound(data([0x65, 0x73, 0x74], endStream: true))
        try channels.response.writeInbound(data([0x89, 0x00, 0x82, 0x02, 0xCA, 0xFE], endStream: true))

        XCTAssertEqual(eventKinds(sink.events), [
            "request", "response:103", "response:200", "open", "frame:text", "requestEnd",
            "frame:ping", "frame:binary", "close", "responseEnd"
        ])
        XCTAssertEqual(webSocketFrames(sink.events).map(\.direction), [
            .clientToServer, .serverToClient, .serverToClient
        ])
        XCTAssertEqual(webSocketFrames(sink.events).map(\.bytes), [Array("test".utf8), [], [0xCA, 0xFE]])
        XCTAssertTrue(responseOpenUsesPermessageDeflate(sink.events))
        XCTAssertFalse(hasHTTPBodyEvent(sink.events))
    }

    func testRejectedHandshakeRemainsHTTPWithoutWebSocketEvents() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 403, endStream: false))
        try channels.request.writeInbound(data(Array("request".utf8), endStream: true))
        try channels.response.writeInbound(data(Array("denied".utf8), endStream: true))

        XCTAssertEqual(eventKinds(sink.events), [
            "request", "response:403", "requestBody", "requestEnd", "responseBody", "responseEnd"
        ])
        XCTAssertFalse(hasWebSocketEvent(sink.events))
    }

    func testCapabilityAbsenceAndMalformedExtendedConnectAreStreamErrors() throws {
        let cases = [
            (headers: extendedConnectHeaders(), enabled: false),
            (headers: extendedConnectHeaders(fields: [("upgrade", "websocket")]), enabled: true)
        ]
        for testCase in cases {
            let sink = RecordingSink()
            let channels = makeChannels(
                requestID: UUID(),
                sink: sink,
                captureLimit: 8,
                extendedConnectEnabled: testCase.enabled
            )
            defer { finish(channels) }

            assertEmpty(try channels.request.writeInbound(testCase.headers))
            XCTAssertEqual(streamErrorCount(sink.events), 1)
            XCTAssertFalse(eventKinds(sink.events).contains("request"))
        }
    }

    func testDataBeforeSuccessfulFinalResponseFailsWithoutBodyCapture() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        _ = try channels.request.readInbound(as: HTTP2Frame.FramePayload.self)
        assertEmpty(try channels.request.writeInbound(data([0x81, 0x00], endStream: false)))

        XCTAssertEqual(streamErrorCount(sink.events), 1)
        XCTAssertFalse(hasHTTPBodyEvent(sink.events))
        XCTAssertFalse(hasWebSocketEvent(sink.events))
    }

    func testOrdinaryCaptureValidationFailureStillForwardsPayload() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)
        defer { finish(channels) }
        let payload = data(Array("ordinary".utf8), endStream: false)

        assertFull(try channels.request.writeInbound(payload))
        let forwarded = try XCTUnwrap(channels.request.readInbound(as: HTTP2Frame.FramePayload.self))

        guard case .data(let frame) = forwarded, case .byteBuffer(let buffer) = frame.data else {
            return XCTFail("Expected forwarded DATA")
        }
        XCTAssertEqual(Array(buffer.readableBytesView), Array("ordinary".utf8))
        XCTAssertEqual(streamErrorCount(sink.events), 1)
    }

    func testDecoderFailureClosesOnceAndDoesNotForwardInvalidData() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200, endStream: false))
        _ = try channels.request.readInbound(as: HTTP2Frame.FramePayload.self)
        _ = try channels.response.readInbound(as: HTTP2Frame.FramePayload.self)
        assertEmpty(try channels.request.writeInbound(data([0x83, 0x00], endStream: false)))

        XCTAssertEqual(streamErrorCount(sink.events), 1)
        XCTAssertEqual(webSocketCloseCount(sink.events), 1)
        XCTAssertTrue(webSocketFrames(sink.events).isEmpty)
    }

    func testCloseFrameResetAndInactivityAreClosureDedupeSignals() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200, endStream: false))
        try channels.request.writeInbound(data([0x88, 0x02, 0x03, 0xE8], endStream: false))
        try channels.request.writeInbound(HTTP2Frame.FramePayload.rstStream(.cancel))
        _ = try? channels.request.finish()
        _ = try? channels.response.finish()

        XCTAssertEqual(webSocketCloseCount(sink.events), 1)
        XCTAssertEqual(streamErrorCount(sink.events), 0)
    }

    func testResetBeforeWebSocketCloseEmitsStreamErrorAndTerminalClose() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200, endStream: false))
        try channels.request.writeInbound(HTTP2Frame.FramePayload.rstStream(.cancel))

        XCTAssertEqual(webSocketCloseCount(sink.events), 1)
        XCTAssertEqual(streamErrorCount(sink.events), 1)
    }

    func testBidirectionalCloseFramesThenInactivityDoNotEmitStreamError() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200, endStream: false))
        try channels.request.writeInbound(data([0x88, 0x02, 0x03, 0xE8], endStream: false))
        try channels.response.writeInbound(data([0x88, 0x02, 0x03, 0xE8], endStream: false))
        _ = try? channels.request.finish()
        _ = try? channels.response.finish()

        XCTAssertEqual(webSocketCloseCount(sink.events), 1)
        XCTAssertEqual(webSocketFrames(sink.events).map(\.opcode), [.connectionClose, .connectionClose])
        XCTAssertEqual(streamErrorCount(sink.events), 0)
    }

    func testErrorAfterWebSocketCloseIsForwardedWithoutCaptureStreamError() throws {
        let sink = RecordingSink()
        let channels = makeChannels(requestID: UUID(), sink: sink, captureLimit: 8)

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200, endStream: false))
        try channels.request.writeInbound(data([0x88, 0x02, 0x03, 0xE8], endStream: false))
        channels.request.pipeline.fireErrorCaught(ExpectedError.teardown)
        _ = try? channels.request.finish()
        _ = try? channels.response.finish()

        XCTAssertEqual(webSocketCloseCount(sink.events), 1)
        XCTAssertEqual(streamErrorCount(sink.events), 0)
    }

    func testConcurrentStreamContextsKeepIdentityFramesAndFailureIsolated() throws {
        let sink = RecordingSink()
        let firstID = UUID()
        let secondID = UUID()
        let first = makeChannels(requestID: firstID, sink: sink, captureLimit: 8)
        let second = makeChannels(requestID: secondID, sink: sink, captureLimit: 8)
        defer {
            finish(first)
            finish(second)
        }

        try first.request.writeInbound(extendedConnectHeaders())
        try second.request.writeInbound(extendedConnectHeaders())
        try first.response.writeInbound(responseHeaders(status: 200, endStream: false))
        try second.response.writeInbound(responseHeaders(status: 200, endStream: false))
        _ = try first.request.readInbound(as: HTTP2Frame.FramePayload.self)
        _ = try first.response.readInbound(as: HTTP2Frame.FramePayload.self)
        _ = try second.request.readInbound(as: HTTP2Frame.FramePayload.self)
        _ = try second.response.readInbound(as: HTTP2Frame.FramePayload.self)
        assertEmpty(try first.request.writeInbound(data([0x83, 0x00], endStream: false)))
        try second.request.writeInbound(data([0x81, 0x02, 0x6F, 0x6B], endStream: false))

        let frameIDs = webSocketFrames(sink.events).map(\.connectionID)
        XCTAssertEqual(frameIDs, [secondID])
        XCTAssertEqual(streamErrorIDs(sink.events), [firstID])
        XCTAssertEqual(webSocketOpenIDs(sink.events), [firstID, secondID])
    }
}

private extension HTTP2WebSocketStreamIntegrationTests {
    private func makeChannels(
        requestID: UUID,
        sink: CaptureEventSink,
        captureLimit: Int,
        extendedConnectEnabled: Bool = true
    ) -> StreamChannels {
        let loop = EmbeddedEventLoop()
        let errorState = HTTP2StreamErrorState()
        let streamContext = NIOLoopBound(
            HTTP2WebSocketStreamContext(
                requestID: requestID,
                sink: sink,
                captureLimit: captureLimit,
                extendedConnectEnabled: extendedConnectEnabled
            ),
            eventLoop: loop
        )
        let request = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .request,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink,
            captureBodyLimit: captureLimit,
            errorState: errorState,
            streamContext: streamContext
        ), loop: loop)
        let response = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .response,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink,
            captureBodyLimit: captureLimit,
            errorState: errorState,
            streamContext: streamContext
        ), loop: loop)
        return StreamChannels(request: request, response: response)
    }

    private func extendedConnectHeaders(fields: [(String, String)] = []) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "CONNECT")
        headers.add(name: ":protocol", value: "websocket")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":authority", value: "example.com")
        headers.add(name: ":path", value: "/socket")
        for (name, value) in fields {
            headers.add(name: name, value: value)
        }
        return .headers(.init(headers: headers, endStream: false))
    }

    private func responseHeaders(
        status: Int,
        endStream: Bool,
        fields: [(String, String)] = []
    ) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: String(status))
        for (name, value) in fields {
            headers.add(name: name, value: value)
        }
        return .headers(.init(headers: headers, endStream: endStream))
    }

    private func data(_ bytes: [UInt8], endStream: Bool) -> HTTP2Frame.FramePayload {
        .data(.init(data: .byteBuffer(ByteBuffer(bytes: bytes)), endStream: endStream))
    }

    private func finish(_ channels: StreamChannels) {
        _ = try? channels.request.finish()
        _ = try? channels.response.finish()
    }

    private func assertEmpty(
        _ state: EmbeddedChannel.BufferState,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case .empty = state else {
            return XCTFail("Expected empty embedded channel buffer", file: file, line: line)
        }
    }

    private func assertFull(
        _ state: EmbeddedChannel.BufferState,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case .full = state else {
            return XCTFail("Expected full embedded channel buffer", file: file, line: line)
        }
    }
}

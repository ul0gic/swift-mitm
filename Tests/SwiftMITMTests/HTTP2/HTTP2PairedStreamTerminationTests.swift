import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOHPACK
import NIOHTTP2
import XCTest

@testable import SwiftMITM

final class HTTP2PairedStreamTerminationTests: XCTestCase {
    private final class DiscardingSink: CaptureEventSink {
        func receive(_ event: CaptureEvent) {}
    }

    private struct GluedChannels {
        let request: EmbeddedChannel
        let response: EmbeddedChannel
    }

    func testMalformedAndCapabilityDisabledConnectCloseBothPairedChildren() throws {
        let cases = [
            (enabled: false, fields: [(String, String)]()),
            (enabled: true, fields: [("upgrade", "websocket")])
        ]
        for testCase in cases {
            let channels = try makeGluedChannels(extendedConnectEnabled: testCase.enabled)
            defer { finish(channels) }

            try channels.request.writeInbound(extendedConnectHeaders(fields: testCase.fields))

            XCTAssertFalse(channels.request.isActive)
            XCTAssertFalse(channels.response.isActive)
        }
    }

    func testPreSuccessDataClosesBothPairedChildren() throws {
        let channels = try makeGluedChannels()
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.request.writeInbound(data([0x81, 0x00]))

        XCTAssertFalse(channels.request.isActive)
        XCTAssertFalse(channels.response.isActive)
    }

    func testWebSocketParserFailureClosesBothPairedChildren() throws {
        let channels = try makeGluedChannels()
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200))
        try channels.request.writeInbound(data([0x83, 0x00]))

        XCTAssertFalse(channels.request.isActive)
        XCTAssertFalse(channels.response.isActive)
    }

    func testFailedExtendedConnectPairDoesNotCloseOrdinaryPair() throws {
        let failed = try makeGluedChannels()
        let ordinary = try makeGluedChannels()
        defer {
            finish(failed)
            finish(ordinary)
        }
        XCTAssertTrue(ordinary.request.isActive)
        XCTAssertTrue(ordinary.response.isActive)

        try failed.request.writeInbound(extendedConnectHeaders())
        try failed.request.writeInbound(data([0x81, 0x00]))
        XCTAssertTrue(ordinary.request.isActive)
        XCTAssertTrue(ordinary.response.isActive)
        let request = ordinaryRequestHeaders()
        let response = responseHeaders(status: 200)
        try ordinary.request.writeInbound(request)
        try ordinary.response.writeInbound(response)

        XCTAssertFalse(failed.request.isActive)
        XCTAssertFalse(failed.response.isActive)
        XCTAssertTrue(ordinary.request.isActive)
        XCTAssertTrue(ordinary.response.isActive)
        XCTAssertNotNil(try ordinary.response.readOutbound(as: HTTP2Frame.FramePayload.self))
        XCTAssertNotNil(try ordinary.request.readOutbound(as: HTTP2Frame.FramePayload.self))
    }

    func testFinalDataInputClosedWaitsForReadCompleteWithoutResettingPartner() throws {
        let channels = try makeGluedChannels()
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200))
        _ = try channels.response.readOutbound(as: HTTP2Frame.FramePayload.self)
        _ = try channels.request.readOutbound(as: HTTP2Frame.FramePayload.self)
        let finalData = data([0x82, 0x00], endStream: true)

        channels.response.pipeline.fireChannelRead(finalData)
        channels.response.pipeline.fireErrorCaught(NIOHTTP2Errors.badStreamStateTransition(
            from: .halfClosedRemoteLocalActive
        ))
        channels.response.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
        channels.response.pipeline.fireErrorCaught(NIOHTTP2Errors.streamClosed(
            streamID: 1,
            errorCode: .cancel
        ))

        XCTAssertTrue(channels.request.isActive)
        XCTAssertTrue(channels.response.isActive)
        XCTAssertNil(try channels.request.readOutbound(as: HTTP2Frame.FramePayload.self))

        channels.response.pipeline.fireChannelReadComplete()

        let forwarded = try XCTUnwrap(channels.request.readOutbound(as: HTTP2Frame.FramePayload.self))
        guard case .data(let frame) = forwarded, case .byteBuffer(let buffer) = frame.data else {
            return XCTFail("Expected final DATA")
        }
        XCTAssertEqual(Array(buffer.readableBytesView), [0x82, 0x00])
        XCTAssertTrue(frame.endStream)
        XCTAssertTrue(channels.request.isActive)
    }

    func testEndStreamInsideWebSocketFrameClosesBothPairedChildren() throws {
        let channels = try makeGluedChannels()
        defer { finish(channels) }

        try channels.request.writeInbound(extendedConnectHeaders())
        try channels.response.writeInbound(responseHeaders(status: 200))
        try channels.response.writeInbound(data([0x82, 0x04, 0xAA], endStream: true))

        XCTAssertFalse(channels.request.isActive)
        XCTAssertFalse(channels.response.isActive)
    }

    private func makeGluedChannels(extendedConnectEnabled: Bool = true) throws -> GluedChannels {
        let loop = EmbeddedEventLoop()
        let sink = DiscardingSink()
        let errorState = HTTP2StreamErrorState()
        let streamContext = NIOLoopBound(
            HTTP2WebSocketStreamContext(
                requestID: UUID(),
                sink: sink,
                captureLimit: 8,
                extendedConnectEnabled: extendedConnectEnabled
            ),
            eventLoop: loop
        )
        let request = EmbeddedChannel(loop: loop)
        let response = EmbeddedChannel(loop: loop)
        let address = try SocketAddress(ipAddress: "127.0.0.1", port: 443)
        try request.connect(to: address).wait()
        try response.connect(to: address).wait()
        streamContext.value.configurePairedStreams(request: request, response: response)
        let pair = GlueHandler.matchedPair(propagateInputClosed: false)
        try request.pipeline.syncOperations.addHandlers([
            HTTP2CaptureTapHandler(
                direction: .request,
                requestID: UUID(),
                authority: "example.com:443",
                sink: sink,
                captureBodyLimit: 8,
                errorState: errorState,
                streamContext: streamContext
            ),
            pair.0
        ])
        try response.pipeline.syncOperations.addHandlers([
            HTTP2CaptureTapHandler(
                direction: .response,
                requestID: UUID(),
                authority: "example.com:443",
                sink: sink,
                captureBodyLimit: 8,
                errorState: errorState,
                streamContext: streamContext
            ),
            pair.1
        ])
        return GluedChannels(request: request, response: response)
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

    private func ordinaryRequestHeaders() -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "GET")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":authority", value: "example.com")
        headers.add(name: ":path", value: "/")
        return .headers(.init(headers: headers, endStream: false))
    }

    private func responseHeaders(status: Int) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: String(status))
        return .headers(.init(headers: headers, endStream: false))
    }

    private func data(_ bytes: [UInt8], endStream: Bool = false) -> HTTP2Frame.FramePayload {
        .data(.init(data: .byteBuffer(ByteBuffer(bytes: bytes)), endStream: endStream))
    }

    private func finish(_ channels: GluedChannels) {
        _ = try? channels.request.finish()
        _ = try? channels.response.finish()
    }
}

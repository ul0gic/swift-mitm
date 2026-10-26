import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class HTTP2InitialSettingsProbeTests: XCTestCase {
    private final class ByteRecordingHandler: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer

        private let storage: NIOLockedValueBox<[[UInt8]]>

        init(storage: NIOLockedValueBox<[[UInt8]]>) {
            self.storage = storage
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            let buffer = unwrapInboundIn(data)
            storage.withLockedValue { $0.append(Array(buffer.readableBytesView)) }
            context.fireChannelRead(data)
        }
    }

    func testEnableConnectProtocolAtEveryTwoChunkBoundaryReplaysBytesExactlyOnce() throws {
        let frame = settingsFrame([
            (identifier: 0x1, value: 4096),
            (identifier: 0x8, value: 1)
        ])

        for boundary in 0 ... frame.readableBytes {
            var probe = HTTP2InitialSettingsProbe()
            let first = frame.getSlice(at: frame.readerIndex, length: boundary) ?? ByteBuffer()
            let second = frame.getSlice(
                at: frame.readerIndex + boundary,
                length: frame.readableBytes - boundary
            ) ?? ByteBuffer()
            var result: HTTP2InitialSettingsProbeResult?

            for var chunk in [first, second] where chunk.readableBytes > 0 {
                switch try probe.receive(&chunk) {
                case .incomplete:
                    break
                case .complete(let completed):
                    result = completed
                }
            }

            XCTAssertEqual(result?.enablesExtendedConnect, true, "boundary \(boundary)")
            XCTAssertEqual(
                result.map { Array($0.replay.readableBytesView) },
                Array(frame.readableBytesView),
                "boundary \(boundary)"
            )
        }
    }

    func testCoalescedBytesAfterInitialSettingsRemainUnconsumed() throws {
        var frame = settingsFrame([(identifier: 0x8, value: 1)], trailing: [0x01, 0x02, 0x03])
        var probe = HTTP2InitialSettingsProbe()
        guard case .complete(let result) = try probe.receive(&frame) else {
            return XCTFail("Expected a complete SETTINGS frame")
        }
        let expectedReplay = settingsFrame([(identifier: 0x8, value: 1)])
        XCTAssertEqual(Array(result.replay.readableBytesView), Array(expectedReplay.readableBytesView))
        XCTAssertEqual(Array(frame.readableBytesView), [0x01, 0x02, 0x03])
    }

    func testMissingAndFinalDisabledEnableConnectProtocolAreUnsupported() throws {
        for settings in [
            [(identifier: UInt16(0x1), value: UInt32(4096))],
            [(identifier: UInt16(0x8), value: UInt32(0))]
        ] {
            var probe = HTTP2InitialSettingsProbe()
            var frame = settingsFrame(settings)
            guard case .complete(let result) = try probe.receive(&frame) else {
                return XCTFail("Expected a complete SETTINGS frame")
            }
            XCTAssertFalse(result.enablesExtendedConnect)
        }
    }

    func testEmptySettingsFrameCompletesWithoutCapability() throws {
        var probe = HTTP2InitialSettingsProbe()
        var frame = settingsFrame([])
        guard case .complete(let result) = try probe.receive(&frame) else {
            return XCTFail("Expected a complete SETTINGS frame")
        }
        XCTAssertFalse(result.enablesExtendedConnect)
    }

    func testDuplicateEnableConnectProtocolCanProgressFromZeroToOneOrRepeatOne() throws {
        for settings in [
            [(identifier: UInt16(0x8), value: UInt32(0)), (identifier: UInt16(0x8), value: UInt32(1))],
            [(identifier: UInt16(0x8), value: UInt32(1)), (identifier: UInt16(0x8), value: UInt32(1))]
        ] {
            var probe = HTTP2InitialSettingsProbe()
            var frame = settingsFrame(settings)
            guard case .complete(let result) = try probe.receive(&frame) else {
                return XCTFail("Expected a complete SETTINGS frame")
            }
            XCTAssertTrue(result.enablesExtendedConnect)
        }
    }

    func testFirstFrameMustBeNonAcknowledgementSettingsOnStreamZero() {
        XCTAssertThrowsError(try probe(frame(type: 0x0))) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .unexpectedFrameType(0x0))
        }
        XCTAssertThrowsError(try probe(frame(flags: 0x1))) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .settingsAcknowledgement)
        }
        XCTAssertThrowsError(try probe(frame(streamIdentifier: 3))) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .settingsStreamIdentifier(3))
        }
    }

    func testSettingsLengthAndEnableConnectValueAreValidated() {
        XCTAssertThrowsError(try probe(frame(payload: [0x00]))) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .invalidSettingsLength)
        }
        XCTAssertThrowsError(try probe(settingsFrame([(identifier: 0x8, value: 2)]))) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .invalidEnableConnectProtocolValue(2))
        }
        XCTAssertThrowsError(try probe(settingsFrame([
            (identifier: 0x8, value: 1),
            (identifier: 0x8, value: 0)
        ]))) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .enableConnectProtocolDisabledAfterEnabled)
        }
    }

    func testAbsoluteBufferLimitRejectsDeclaredExcessWithoutConsumingCoalescedBytes() throws {
        var declared = ByteBuffer()
        declared.writeBytes([0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00])
        XCTAssertThrowsError(try probe(declared)) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .bufferLimitExceeded)
        }

        var probe = HTTP2InitialSettingsProbe()
        let trailing = Array(repeating: UInt8(0), count: HTTP2InitialSettingsProbe.maximumBufferedBytes + 1)
        var coalesced = settingsFrame([], trailing: trailing)
        guard case .complete = try probe.receive(&coalesced) else { return XCTFail("Expected complete settings") }
        XCTAssertEqual(coalesced.readableBytes, HTTP2InitialSettingsProbe.maximumBufferedBytes + 1)
    }

    func testCompletionCanOnlyReleaseReplayOnceAndDoesNotInterpretLaterSettings() throws {
        var probe = HTTP2InitialSettingsProbe()
        var first = settingsFrame([])
        _ = try probe.receive(&first)
        var second = settingsFrame([(identifier: 0x8, value: 1)])
        XCTAssertThrowsError(try probe.receive(&second)) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .alreadyCompleted)
        }
    }

    func testOwningBoundaryUsesFiveSecondDeadline() {
        XCTAssertEqual(HTTP2InitialSettingsProbeBoundary.deadline, .seconds(5))
    }

    func testHandlerResolvesCapabilityReplaysSettingsAndPassesCoalescedBytesOnce() throws {
        let loop = EmbeddedEventLoop()
        let handler = HTTP2InitialSettingsProbeHandler(eventLoop: loop)
        let channel = EmbeddedChannel(handler: handler, loop: loop)
        defer { _ = try? channel.finish() }
        let settings = settingsFrame([(identifier: 0x8, value: 1)])
        let following = ByteBuffer(bytes: [0x00, 0x00, 0x00, 0x04])
        var coalesced = settings
        var followingCopy = following
        coalesced.writeBuffer(&followingCopy)

        assertFull(try channel.writeInbound(coalesced))
        XCTAssertTrue(try handler.capability.wait())

        let replay = try XCTUnwrap(channel.readInbound(as: ByteBuffer.self))
        let remaining = try XCTUnwrap(channel.readInbound(as: ByteBuffer.self))
        XCTAssertEqual(Array(replay.readableBytesView), Array(settings.readableBytesView))
        XCTAssertEqual(Array(remaining.readableBytesView), Array(following.readableBytesView))
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))

        let later = ByteBuffer(bytes: [0xAA])
        assertFull(try channel.writeInbound(later))
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), [0xAA])
    }

    func testCapabilityObserverRunsAfterReplayForwardingAndProbeRemoval() throws {
        let loop = EmbeddedEventLoop()
        let handler = HTTP2InitialSettingsProbeHandler(eventLoop: loop)
        let received = NIOLockedValueBox<[[UInt8]]>([])
        let observed = NIOLockedValueBox<(received: [[UInt8]], probeRemoved: Bool)?>(nil)
        let channel = EmbeddedChannel(loop: loop)
        try channel.pipeline.syncOperations.addHandlers([
            handler,
            ByteRecordingHandler(storage: received)
        ])
        defer { _ = try? channel.finish() }
        let boundHandler = NIOLoopBound(handler, eventLoop: loop)
        handler.capability.whenSuccess { _ in
            let probeRemoved = (try? channel.pipeline.syncOperations.context(handler: boundHandler.value)) == nil
            observed.withLockedValue { value in
                value = (received.withLockedValue { $0 }, probeRemoved)
            }
        }
        let settings = settingsFrame([(identifier: 0x8, value: 1)])
        let following = [UInt8(0x00), 0x00, 0x00, 0x04]
        var coalesced = settings
        coalesced.writeBytes(following)

        assertFull(try channel.writeInbound(coalesced))
        XCTAssertTrue(try handler.capability.wait())

        let callbackObservation = try XCTUnwrap(observed.withLockedValue { $0 })
        XCTAssertEqual(callbackObservation.received, [Array(settings.readableBytesView), following])
        XCTAssertTrue(callbackObservation.probeRemoved)
    }

    func testHandlerDeadlineFailsCapabilityAndCloses() throws {
        let loop = EmbeddedEventLoop()
        let handler = HTTP2InitialSettingsProbeHandler(eventLoop: loop)
        let channel = EmbeddedChannel(handler: handler, loop: loop)
        defer { _ = try? channel.finish() }

        loop.advanceTime(by: .seconds(5))

        XCTAssertThrowsError(try handler.capability.wait()) { error in
            XCTAssertEqual(error as? HTTP2InitialSettingsProbeError, .deadlineExceeded)
        }
        XCTAssertFalse(channel.isActive)
    }

    private func probe(_ bytes: ByteBuffer) throws {
        var probe = HTTP2InitialSettingsProbe()
        var bytes = bytes
        _ = try probe.receive(&bytes)
    }

    private func settingsFrame(
        _ settings: [(identifier: UInt16, value: UInt32)],
        trailing: [UInt8] = []
    ) -> ByteBuffer {
        var payload = ByteBuffer()
        for setting in settings {
            payload.writeInteger(setting.identifier, endianness: .big)
            payload.writeInteger(setting.value, endianness: .big)
        }
        var bytes = frame(payload: Array(payload.readableBytesView))
        bytes.writeBytes(trailing)
        return bytes
    }

    private func frame(
        type: UInt8 = 0x4,
        flags: UInt8 = 0,
        streamIdentifier: UInt32 = 0,
        payload: [UInt8] = []
    ) -> ByteBuffer {
        var bytes = ByteBuffer()
        bytes.writeInteger(UInt8((payload.count >> 16) & 0xFF))
        bytes.writeInteger(UInt8((payload.count >> 8) & 0xFF))
        bytes.writeInteger(UInt8(payload.count & 0xFF))
        bytes.writeInteger(type)
        bytes.writeInteger(flags)
        bytes.writeInteger(streamIdentifier, endianness: .big)
        bytes.writeBytes(payload)
        return bytes
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

import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class OpaqueFlowBridgeHandlerTests: XCTestCase {
    private final class ReadRecorder: ChannelOutboundHandler {
        typealias OutboundIn = ByteBuffer
        typealias OutboundOut = ByteBuffer

        private(set) var readCount = 0

        func read(context: ChannelHandlerContext) {
            readCount += 1
            context.read()
        }
    }

    private final class HalfCloseRecorder: ChannelOutboundHandler {
        typealias OutboundIn = ByteBuffer
        typealias OutboundOut = ByteBuffer

        private(set) var outputCloseCount = 0

        func close(
            context: ChannelHandlerContext,
            mode: CloseMode,
            promise: EventLoopPromise<Void>?
        ) {
            if mode == .output {
                outputCloseCount += 1
                promise?.succeed(())
            } else {
                context.close(mode: mode, promise: promise)
            }
        }
    }

    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    private struct Bridge {
        let client: EmbeddedChannel
        let server: EmbeddedChannel
        let clientHandler: OpaqueFlowBridgeHandler
        let serverHandler: OpaqueFlowBridgeHandler
        let clientReadRecorder: ReadRecorder
        let clientHalfCloseRecorder: HalfCloseRecorder
        let serverHalfCloseRecorder: HalfCloseRecorder
        let sink: RecordingSink
    }

    func testClientFirstServerFirstAndBidirectionalBytesForwardExactlyWithBoundedCapture() throws {
        let bridge = try makeBridge(captureByteLimit: 2)
        defer { finish(bridge) }

        try bridge.client.writeInbound(ByteBuffer(bytes: [1, 2, 3]))
        try bridge.server.writeInbound(ByteBuffer(bytes: [4, 5, 6]))
        try bridge.client.writeInbound(ByteBuffer(bytes: [7]))

        XCTAssertEqual(try readBytes(from: bridge.server), [1, 2, 3])
        XCTAssertEqual(try readBytes(from: bridge.client), [4, 5, 6])
        XCTAssertEqual(try readBytes(from: bridge.server), [7])
        XCTAssertEqual(dataBytes(bridge.sink.events), [[1, 2], [4, 5], []])
        XCTAssertEqual(dataCounts(bridge.sink.events), [3, 3, 1])
        XCTAssertEqual(eventKinds(bridge.sink.events).first, "open")
    }

    func testBidirectionalHalfCloseEmitsEndsThenOneCompletedClose() throws {
        let bridge = try makeBridge(captureByteLimit: 8)
        defer { finish(bridge) }

        try bridge.server.writeInbound(ByteBuffer(bytes: [9]))
        bridge.client.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
        bridge.client.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
        bridge.server.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)

        XCTAssertEqual(eventKinds(bridge.sink.events), ["open", "data", "end", "end", "close"])
        XCTAssertEqual(endDirections(bridge.sink.events), [.clientToServer, .serverToClient])
        XCTAssertEqual(closeReasons(bridge.sink.events), [.completed])
        XCTAssertEqual(bridge.clientHalfCloseRecorder.outputCloseCount, 1)
        XCTAssertEqual(bridge.serverHalfCloseRecorder.outputCloseCount, 1)
        XCTAssertTrue(bridge.client.isActive)
        XCTAssertTrue(bridge.server.isActive)
    }

    func testInactivePartnerAfterObservedHalfCloseCompletesBothDirections() throws {
        let bridge = try makeBridge(captureByteLimit: 8)
        defer { finish(bridge) }

        bridge.server.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
        bridge.client.pipeline.fireChannelInactive()

        XCTAssertEqual(endDirections(bridge.sink.events), [.serverToClient, .clientToServer])
        XCTAssertEqual(closeReasons(bridge.sink.events), [.completed])
        XCTAssertTrue(errorReasons(bridge.sink.events).isEmpty)
    }

    func testAbruptFailureClosesBothSidesAndEmitsOneError() throws {
        enum TestError: Error { case failed }
        let bridge = try makeBridge()
        defer { finish(bridge) }

        bridge.client.pipeline.fireErrorCaught(TestError.failed)
        bridge.server.pipeline.fireErrorCaught(TestError.failed)

        XCTAssertFalse(bridge.client.isActive)
        XCTAssertFalse(bridge.server.isActive)
        XCTAssertEqual(errorReasons(bridge.sink.events), [.transportFailure])
        XCTAssertTrue(closeReasons(bridge.sink.events).isEmpty)
    }

    func testCancellationClosesBothSidesWithOneCancelledClose() throws {
        let bridge = try makeBridge()
        defer { finish(bridge) }

        bridge.clientHandler.cancel()
        bridge.clientHandler.cancel()

        XCTAssertFalse(bridge.client.isActive)
        XCTAssertFalse(bridge.server.isActive)
        XCTAssertEqual(closeReasons(bridge.sink.events), [.cancelled])
        XCTAssertTrue(errorReasons(bridge.sink.events).isEmpty)
    }

    func testReadDemandWaitsUntilPartnerBecomesWritable() throws {
        let bridge = try makeBridge()
        defer { finish(bridge) }
        bridge.serverHandler.updateWritability(false)
        let initialReadCount = bridge.clientReadRecorder.readCount

        bridge.client.read()

        XCTAssertEqual(bridge.clientReadRecorder.readCount, initialReadCount)
        bridge.serverHandler.updateWritability(true)
        XCTAssertEqual(bridge.clientReadRecorder.readCount, initialReadCount + 1)
    }

    private func makeBridge(captureByteLimit: Int = 0) throws -> Bridge {
        let loop = EmbeddedEventLoop()
        let sink = RecordingSink()
        let flow = CapturedOpaqueFlow(
            id: UUID(),
            timestamp: Date(),
            target: CapturedTarget(
                destination: CapturedNetworkEndpoint(address: "192.0.2.10", port: 443),
                logicalAuthority: "192.0.2.10:443",
                tlsServerName: nil,
                ingressProvenance: .trustedProxyV2,
                originalClient: nil
            )
        )
        let handlers = OpaqueFlowBridgeHandler.matchedPair(
            flow: flow,
            sink: sink,
            captureByteLimit: captureByteLimit,
            eventLoop: loop
        )
        let client = EmbeddedChannel(loop: loop)
        let server = EmbeddedChannel(loop: loop)
        let clientReadRecorder = ReadRecorder()
        let clientHalfCloseRecorder = HalfCloseRecorder()
        let serverHalfCloseRecorder = HalfCloseRecorder()
        let address = try SocketAddress(ipAddress: "127.0.0.1", port: 443)
        try client.connect(to: address).wait()
        try server.connect(to: address).wait()
        try client.pipeline.syncOperations.addHandlers([
            clientHalfCloseRecorder,
            clientReadRecorder,
            handlers.0
        ])
        try server.pipeline.syncOperations.addHandlers([serverHalfCloseRecorder, handlers.1])
        return Bridge(
            client: client,
            server: server,
            clientHandler: handlers.0,
            serverHandler: handlers.1,
            clientReadRecorder: clientReadRecorder,
            clientHalfCloseRecorder: clientHalfCloseRecorder,
            serverHalfCloseRecorder: serverHalfCloseRecorder,
            sink: sink
        )
    }

    private func readBytes(from channel: EmbeddedChannel) throws -> [UInt8]? {
        try channel.readOutbound(as: ByteBuffer.self).map { Array($0.readableBytesView) }
    }

    private func finish(_ bridge: Bridge) {
        _ = try? bridge.client.finish()
        _ = try? bridge.server.finish()
    }

    private func eventKinds(_ events: [CaptureEvent]) -> [String] {
        events.compactMap { event in
            switch event {
            case .opaqueOpen: "open"
            case .opaqueData: "data"
            case .opaqueDirectionEnd: "end"
            case .opaqueClose: "close"
            case .opaqueError: "error"
            default: nil
            }
        }
    }

    private func dataBytes(_ events: [CaptureEvent]) -> [[UInt8]] {
        events.compactMap { event in
            guard case .opaqueData(_, _, _, let bytes, _) = event else { return nil }
            return bytes
        }
    }

    private func dataCounts(_ events: [CaptureEvent]) -> [Int] {
        events.compactMap { event in
            guard case .opaqueData(_, _, _, _, let byteCount) = event else { return nil }
            return byteCount
        }
    }

    private func endDirections(_ events: [CaptureEvent]) -> [OpaqueFlowDirection] {
        events.compactMap { event in
            guard case .opaqueDirectionEnd(_, _, let direction, _, _) = event else { return nil }
            return direction
        }
    }

    private func closeReasons(_ events: [CaptureEvent]) -> [OpaqueFlowCloseReason] {
        events.compactMap { event in
            guard case .opaqueClose(_, _, let reason) = event else { return nil }
            return reason
        }
    }

    private func errorReasons(_ events: [CaptureEvent]) -> [CapturedConnectionFailureReason] {
        events.compactMap { event in
            guard case .opaqueError(_, _, let reason) = event else { return nil }
            return reason
        }
    }
}

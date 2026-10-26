import Foundation
import NIOConcurrencyHelpers
import NIOCore
import XCTest

@testable import SwiftMITM

final class OpaqueCaptureSessionTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    func testIndependentDirectionalBoundsReportObservedChunkAndCumulativeCounts() {
        let sink = RecordingSink()
        let flow = makeFlow()
        let session = OpaqueCaptureSession(flow: flow, sink: sink, captureByteLimit: 2)

        XCTAssertTrue(session.capture(ByteBuffer(bytes: [1, 2, 3]), direction: .clientToServer))
        XCTAssertTrue(session.capture(ByteBuffer(bytes: [4]), direction: .clientToServer))
        XCTAssertTrue(session.capture(ByteBuffer(bytes: [5, 6, 7]), direction: .serverToClient))
        session.directionEnd(.clientToServer)
        session.directionEnd(.serverToClient)

        XCTAssertEqual(eventKinds(sink.events), ["open", "data", "data", "data", "end", "end", "close"])
        XCTAssertEqual(dataEvents(sink.events).map(\.bytes), [[1, 2], [], [5, 6]])
        XCTAssertEqual(dataEvents(sink.events).map(\.byteCount), [3, 1, 3])
        XCTAssertEqual(endEvents(sink.events).map(\.byteCount), [4, 3])
        XCTAssertEqual(endEvents(sink.events).map(\.truncated), [true, true])
        XCTAssertEqual(closeReasons(sink.events), [.completed])
    }

    func testZeroLimitAndZeroByteFlowRemainMetadataOnly() {
        let sink = RecordingSink()
        let session = OpaqueCaptureSession(flow: makeFlow(), sink: sink)

        XCTAssertTrue(session.capture(ByteBuffer(bytes: [1, 2]), direction: .serverToClient))
        session.directionEnd(.serverToClient)
        session.directionEnd(.clientToServer)

        XCTAssertEqual(dataEvents(sink.events).map(\.bytes), [[]])
        XCTAssertEqual(dataEvents(sink.events).map(\.byteCount), [2])
        XCTAssertEqual(endEvents(sink.events).map(\.byteCount), [2, 0])
        XCTAssertEqual(endEvents(sink.events).map(\.truncated), [true, false])
    }

    func testDirectionAndTerminalEventsAreDeduplicated() {
        let sink = RecordingSink()
        let session = OpaqueCaptureSession(flow: makeFlow(), sink: sink, captureByteLimit: 8)

        session.directionEnd(.clientToServer)
        session.directionEnd(.clientToServer)
        session.fail(reason: .transportFailure)
        session.fail(reason: .timedOut)
        session.close(reason: .cancelled)

        XCTAssertEqual(eventKinds(sink.events), ["open", "end", "error"])
        XCTAssertFalse(session.capture(ByteBuffer(bytes: [1]), direction: .serverToClient))
    }

    private func makeFlow() -> CapturedOpaqueFlow {
        CapturedOpaqueFlow(
            id: UUID(),
            timestamp: Date(),
            target: CapturedTarget(
                destination: CapturedNetworkEndpoint(address: "192.0.2.10", port: 443),
                logicalAuthority: "example.com:443",
                tlsServerName: "example.com",
                ingressProvenance: .trustedProxyV2,
                originalClient: CapturedNetworkEndpoint(address: "192.0.2.20", port: 50_000)
            )
        )
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

    private func dataEvents(_ events: [CaptureEvent]) -> [(bytes: [UInt8], byteCount: Int)] {
        events.compactMap { event in
            guard case let .opaqueData(_, _, _, bytes, byteCount) = event else { return nil }
            return (bytes, byteCount)
        }
    }

    private func endEvents(_ events: [CaptureEvent]) -> [(byteCount: Int, truncated: Bool)] {
        events.compactMap { event in
            guard case let .opaqueDirectionEnd(_, _, _, byteCount, truncated) = event else { return nil }
            return (byteCount, truncated)
        }
    }

    private func closeReasons(_ events: [CaptureEvent]) -> [OpaqueFlowCloseReason] {
        events.compactMap { event in
            guard case .opaqueClose(_, _, let reason) = event else { return nil }
            return reason
        }
    }
}

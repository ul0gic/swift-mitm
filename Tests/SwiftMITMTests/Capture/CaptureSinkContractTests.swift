import Dispatch
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class CaptureSinkContractTests: XCTestCase {
    private final class RecordingSink: CaptureEventSink {
        private let storage = NIOLockedValueBox<[CaptureEvent]>([])

        var events: [CaptureEvent] { storage.withLockedValue { $0 } }

        func receive(_ event: CaptureEvent) {
            storage.withLockedValue { $0.append(event) }
        }
    }

    private final class BlockingSink: CaptureEventSink {
        private struct State {
            var events: [CaptureEvent] = []
            var timedOut = false
        }

        private let state = NIOLockedValueBox(State())
        private let entered = DispatchSemaphore(value: 0)
        private let releaseSignal = DispatchSemaphore(value: 0)

        var events: [CaptureEvent] { state.withLockedValue { $0.events } }
        var timedOut: Bool { state.withLockedValue { $0.timedOut } }

        func receive(_ event: CaptureEvent) {
            let shouldBlock = state.withLockedValue { state in
                state.events.append(event)
                return state.events.count == 1
            }
            guard shouldBlock else { return }
            entered.signal()
            if releaseSignal.wait(timeout: .now() + 5) == .timedOut {
                state.withLockedValue { $0.timedOut = true }
            }
        }

        func waitUntilEntered() -> DispatchTimeoutResult {
            entered.wait(timeout: .now() + 2)
        }

        func release() {
            releaseSignal.signal()
        }
    }

    private final class ConcurrentRecordingSink: CaptureEventSink {
        private struct State {
            var events: [CaptureEvent] = []
            var rendezvousEntries = 0
            var timedOut = false
        }

        private let state = NIOLockedValueBox(State())
        private let entered = DispatchSemaphore(value: 0)
        private let releaseSignal = DispatchSemaphore(value: 0)

        var events: [CaptureEvent] { state.withLockedValue { $0.events } }
        var timedOut: Bool { state.withLockedValue { $0.timedOut } }

        func receive(_ event: CaptureEvent) {
            let shouldRendezvous = state.withLockedValue { state in
                state.events.append(event)
                guard state.rendezvousEntries < 2 else { return false }
                state.rendezvousEntries += 1
                return true
            }
            guard shouldRendezvous else { return }
            entered.signal()
            if releaseSignal.wait(timeout: .now() + 5) == .timedOut {
                state.withLockedValue { $0.timedOut = true }
            }
        }

        func waitForTwoEntries() -> Bool {
            entered.wait(timeout: .now() + 2) == .success &&
                entered.wait(timeout: .now() + 2) == .success
        }

        func releaseBoth() {
            releaseSignal.signal()
            releaseSignal.signal()
        }
    }

    func testReceiveCompletesBeforeEmbeddedWriteReturns() throws {
        let sink = RecordingSink()
        let channel = Self.requestChannel(sink: sink)
        defer { _ = try? channel.finish() }

        _ = try channel.writeInbound(buffer("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
        XCTAssertEqual(eventKinds(sink.events), ["head", "end"])
    }

    func testOneStreamEmitsHeadBodyTrailersEndInOrder() throws {
        let sink = RecordingSink()
        let channel = Self.requestChannel(sink: sink)
        defer { _ = try? channel.finish() }

        _ = try channel.writeInbound(buffer(
            "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nDigest: value\r\n\r\n"
        ))

        XCTAssertEqual(eventKinds(sink.events), ["head", "body", "trailers", "end"])
    }

    func testBlockedSinkBlocksHandlerWithoutBufferingLaterEvents() {
        let sink = BlockingSink()
        let completed = DispatchSemaphore(value: 0)
        let outcome = NIOLockedValueBox<String?>(nil)
        let input = buffer(
            "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nDigest: value\r\n\r\n"
        )
        defer {
            sink.release()
        }

        DispatchQueue.global().async {
            let channel = Self.requestChannel(sink: sink)
            defer { _ = try? channel.finish() }
            do {
                _ = try channel.writeInbound(input)
                outcome.withLockedValue { $0 = "success" }
            } catch {
                outcome.withLockedValue { $0 = String(describing: error) }
            }
            completed.signal()
        }

        XCTAssertEqual(sink.waitUntilEntered(), .success)
        XCTAssertEqual(eventKinds(sink.events), ["head"])
        XCTAssertEqual(completed.wait(timeout: .now()), .timedOut)

        sink.release()
        XCTAssertEqual(completed.wait(timeout: .now() + 2), .success)
        XCTAssertEqual(outcome.withLockedValue { $0 }, "success")
        XCTAssertFalse(sink.timedOut)
        XCTAssertEqual(eventKinds(sink.events), ["head", "body", "trailers", "end"])
    }

    func testSendableSinkReceivesConcurrentIndependentChannels() {
        let sink = ConcurrentRecordingSink()
        let completed = DispatchGroup()
        let failures = NIOLockedValueBox<[String]>([])
        let firstInput = buffer("GET /first HTTP/1.1\r\nHost: example.com\r\n\r\n")
        let secondInput = buffer("GET /second HTTP/1.1\r\nHost: example.com\r\n\r\n")
        defer {
            sink.releaseBoth()
        }

        for input in [firstInput, secondInput] {
            completed.enter()
            DispatchQueue.global().async {
                defer { completed.leave() }
                let channel = Self.requestChannel(sink: sink)
                defer { _ = try? channel.finish() }
                do {
                    _ = try channel.writeInbound(input)
                } catch {
                    failures.withLockedValue { $0.append(String(describing: error)) }
                }
            }
        }

        XCTAssertTrue(sink.waitForTwoEntries())
        sink.releaseBoth()
        XCTAssertEqual(completed.wait(timeout: .now() + 2), .success)
        XCTAssertEqual(failures.withLockedValue { $0 }, [])
        XCTAssertFalse(sink.timedOut)

        let heads = sink.events.compactMap { event -> UUID? in
            if case .requestHead(let head) = event {
                return head.id
            }
            return nil
        }
        let ends = sink.events.compactMap { event -> UUID? in
            if case .requestEnd(let requestID, _) = event {
                return requestID
            }
            return nil
        }
        XCTAssertEqual(Set(heads), Set(ends))
        XCTAssertEqual(heads.count, 2)
        XCTAssertEqual(ends.count, 2)
    }

    private static func requestChannel(sink: CaptureEventSink) -> EmbeddedChannel {
        EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .request,
            authority: "example.com:443",
            correlator: HTTP1ExchangeCorrelator(),
            sink: sink,
            captureBodyLimit: 32
        ))
    }

    private func buffer(_ value: String) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: value.utf8.count)
        buffer.writeString(value)
        return buffer
    }

    private func eventKinds(_ events: [CaptureEvent]) -> [String] {
        events.compactMap { event in
            switch event {
            case .requestHead: "head"
            case .requestBodyChunk: "body"
            case .requestTrailers: "trailers"
            case .requestEnd: "end"
            case .responseHead, .responseBodyChunk, .responseTrailers, .responseEnd,
                 .streamError, .webSocketOpen, .webSocketFrame, .webSocketClose,
                 .opaqueOpen, .opaqueData, .opaqueDirectionEnd, .opaqueClose,
                 .opaqueError, .connectionFailure: nil
            }
        }
    }
}

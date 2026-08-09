import Foundation
import SwiftMITM
import XCTest
import os

final class DocumentationExamplesTests: XCTestCase {
    private struct DiscardingSink: CaptureEventSink {
        func receive(_: CaptureEvent) {}
    }

    private struct BoundedCaptureSink: CaptureEventSink {
        let continuation: AsyncStream<CaptureEvent>.Continuation
        let recordDrop: @Sendable () -> Void

        func receive(_ event: CaptureEvent) {
            switch continuation.yield(event) {
            case .enqueued:
                break
            case .dropped:
                recordDrop()
            case .terminated:
                break
            @unknown default:
                break
            }
        }
    }

    private final class CaptureMetrics: Sendable {
        private let droppedEvents = OSAllocatedUnfairLock(initialState: 0)

        func recordDrop() {
            droppedEvents.withLock { $0 += 1 }
        }

        var dropCount: Int {
            droppedEvents.withLock { $0 }
        }
    }

    func testGettingStartedGenerationRestorationStartAndStop() async throws {
        let generated = try CertificateAuthority.generate(commonName: "Documentation Test Root")
        let restored = try CertificateAuthority(
            privateKeyPEM: generated.privateKeyPEM,
            certificatePEM: generated.certificatePEM
        )
        let proxy = ProxyServer(certificateAuthority: restored, sink: DiscardingSink())

        let port = try await proxy.start(port: 0)
        XCTAssertGreaterThan(port, 0)
        try await proxy.stop()
    }

    func testBoundedCaptureSinkRetainsOldestEvent() async {
        let requestID = UUID()
        let (events, continuation) = AsyncStream.makeStream(
            of: CaptureEvent.self,
            bufferingPolicy: .bufferingOldest(1)
        )
        let metrics = CaptureMetrics()
        let sink = BoundedCaptureSink(continuation: continuation, recordDrop: metrics.recordDrop)
        sink.receive(.requestEnd(requestID: requestID, truncated: false))
        sink.receive(.requestEnd(requestID: UUID(), truncated: false))
        continuation.finish()

        var iterator = events.makeAsyncIterator()
        guard case .requestEnd(let retainedID, false) = await iterator.next() else {
            return XCTFail("the bounded sink did not retain the oldest event")
        }
        XCTAssertEqual(retainedID, requestID)
        XCTAssertEqual(metrics.dropCount, 1)
        let next = await iterator.next()
        XCTAssertNil(next)
    }
}

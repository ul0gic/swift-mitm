import Dispatch
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import XCTest

import SwiftMITM

private final class Stage2BlockedControlSink: HTTPControlEventSink {
    private struct State { var entered = false; var released = false }
    private let state = NIOLockedValueBox(State())
    private let releaseSignal = DispatchSemaphore(value: 0)

    var entered: Bool { state.withLockedValue { $0.entered } }

    func receive(_ event: HTTPControlEvent) {
        guard case .breakpoint = event else { return }
        state.withLockedValue { $0.entered = true }
        releaseSignal.wait()
    }

    func release() {
        let signal = state.withLockedValue { state in
            guard !state.released else { return false }
            state.released = true
            return true
        }
        if signal {
            releaseSignal.signal()
        }
    }
}

final class Stage2CancellationAcceptanceTests: XCTestCase {
    func testStopReportsNoncooperativeCallbackUntilItsTaskAndBytesActuallyExit() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(id: "orphan", match: HTTPRuleMatch(phase: .request, path: "/blocked"),
                     action: .breakpoint)
        ], limits: HTTPControlLimits(maximumTasks: 2)))
        let blocked = Stage2BlockedControlSink()
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        defer { blocked.release() }
        try await stage2WithHTTPOrigin(runtime: runtime, controlSink: blocked, admission: admission) { peer in
            let received = Release022WireObservation()
            let client = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/blocked HTTP/1.1\r\nHost: localhost\r\nX-Snapshot: retained\r\n\r\n")))
            try await stage2Await { blocked.entered }
            try await client.close().get()
            try await stage2AwaitAsync { await runtime.resourceUsage().orphanedCallbacks == 1 }
            let stopping = NIOLockedValueBox(false)
            let proxy = peer.proxy
            let stop = Task {
                try await proxy.stop()
                stopping.withLockedValue { $0 = true }
            }
            do {
                try await stage2Await { stopping.withLockedValue { $0 } }
                let orphan = await runtime.resourceUsage()
                XCTAssertEqual(orphan.orphanedCallbacks, 1)
                XCTAssertEqual(orphan.callbackTasks, 1)
                XCTAssertGreaterThan(orphan.retainedBytes, 0)
                XCTAssertGreaterThan(admission.usage.retainedBytes, 0)
                XCTAssertEqual(admission.usage.connections, 0)
                XCTAssertEqual(admission.usage.streams, 0)
                XCTAssertTrue(peer.origin.requests.isEmpty)
                blocked.release()
                try await stop.value
                try await stage2AwaitAsync {
                    let usage = await runtime.resourceUsage()
                    return usage.callbackTasks == 0 && usage.retainedBytes == 0 && admission.usage.retainedBytes == 0
                }
            } catch {
                let failure = error
                blocked.release()
                do { try await stop.value } catch { XCTFail("Stop failed after callback release: \(error)") }
                throw failure
            }
        }
        let usage = await runtime.resourceUsage()
        XCTAssertEqual(usage.orphanedCallbacks, 0)
        XCTAssertEqual(usage.retainedBytes, 0)
    }

    func testRestartWithSharedEventLoopRetainsOrphanAccountingAndServesNewTraffic() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(id: "restart-orphan", match: HTTPRuleMatch(phase: .request, path: "/blocked"), action: .breakpoint)
        ], limits: HTTPControlLimits(maximumTasks: 2)))
        let blocked = Stage2BlockedControlSink()
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        defer { blocked.release() }
        try await stage2WithHTTPOrigin(runtime: runtime, controlSink: blocked, admission: admission) { peer in
            let first = try release022WireClient(
                group: peer.group, port: peer.proxyPort, observation: Release022WireObservation()
            )
            try phase4BoundedWait(first.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/blocked HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            try await stage2Await { blocked.entered }
            try await first.close().get()
            try await stage2AwaitAsync { await runtime.resourceUsage().orphanedCallbacks == 1 }
            try await peer.proxy.stop()
            let retainedBeforeRestart = admission.usage.retainedBytes
            XCTAssertGreaterThan(retainedBeforeRestart, 0)
            let port = try await peer.proxy.start(port: 0)
            let restarted = await runtime.resourceUsage()
            XCTAssertEqual(restarted.orphanedCallbacks, 1)
            XCTAssertEqual(restarted.callbackTasks, 1)
            XCTAssertEqual(admission.usage.retainedBytes, retainedBeforeRestart)
            let received = Release022WireObservation()
            let next = try release022WireClient(group: peer.group, port: port, observation: received)
            try phase4BoundedWait(next.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/healthy HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try received.wait { String(decoding: $0, as: UTF8.self).hasPrefix("HTTP/1.1 200 ") }
            XCTAssertEqual(peer.origin.requests.map(\.head.uri), ["/healthy"])
            try await next.close().get()
            try await peer.proxy.stop()
            XCTAssertGreaterThan(admission.usage.retainedBytes, 0)
            blocked.release()
            try await stage2AwaitAsync {
                let usage = await runtime.resourceUsage()
                return usage.callbackTasks == 0 && usage.retainedBytes == 0 && admission.usage.retainedBytes == 0
            }
        }
    }
}

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

private final class Stage2RestartDiagnostics {
    let firstWire = Release022WireObservation()
    let healthyWire = Release022WireObservation()
    var usage: HTTPControlResourceUsage?
    var callbackReleased = false
    var stopCompleted: Bool?
    private let scenario: String
    private let started = DispatchTime.now().uptimeNanoseconds
    private var phaseStarted = DispatchTime.now().uptimeNanoseconds
    private var phase = "initializing"

    init(scenario: String = "Restart") {
        self.scenario = scenario
    }

    func begin(_ name: String) {
        phase = name
        phaseStarted = DispatchTime.now().uptimeNanoseconds
    }

    func report(peer: Stage2HTTPContext, blocked: Stage2BlockedControlSink, admission: ResourceAdmissionController) {
        let now = DispatchTime.now().uptimeNanoseconds
        let current = admission.usage
        let observed = usage.map {
            "callbacks=\($0.callbackTasks),orphans=\($0.orphanedCallbacks),bytes=\($0.retainedBytes),"
                + "slots=\($0.taskSlots),paused=\($0.pausedExchanges)"
        } ?? "not-observed"
        let events = peer.raw.events
        let kinds = events.prefix(64).reduce(into: [String: Int]()) { counts, event in
            let kind: String
            switch event {
            case .requestHead: kind = "requestHead"
            case .responseHead: kind = "responseHead"
            case .streamError, .connectionFailure: kind = "error"
            default: kind = "other"
            }
            counts[kind, default: 0] += 1
        }
        let healthy = healthyWire.bytes
        let healthyStatus200 = healthy.starts(with: Array("HTTP/1.1 200 ".utf8))
        let stopped = stopCompleted.map { $0 ? "true" : "false" } ?? "not-observed"
        XCTFail("\(scenario) acceptance phase=\(phase) elapsedNs=\(now - started) "
            + "phaseElapsedNs=\(now - phaseStarted) "
            + "callbackEntered=\(blocked.entered) callbackReleased=\(callbackReleased) runtimeLast={\(observed)} "
            + "stopCompleted=\(stopped) "
            + "admissionConnections=\(current.connections) admissionStreams=\(current.streams) "
            + "admissionBytes=\(current.retainedBytes) originRequests=\(peer.origin.requests.count) "
            + "firstWireBytes=\(firstWire.bytes.count) healthyWireBytes=\(healthy.count) "
            + "healthyStatus200=\(healthyStatus200) "
            + "rawEvents=\(events.count) first64EventKinds=\(kinds)")
    }
}

final class Stage2CancellationAcceptanceTests: XCTestCase {
    func testStopReportsNoncooperativeCallbackUntilItsTaskAndBytesActuallyExit() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(id: "orphan", match: HTTPRuleMatch(phase: .request, path: "/blocked"), action: .breakpoint)
        ], limits: HTTPControlLimits(maximumTasks: 2)))
        let blocked = Stage2BlockedControlSink()
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        let diagnostics = Stage2RestartDiagnostics(scenario: "Stop")
        defer { blocked.release() }
        try await stage2WithHTTPOrigin(runtime: runtime, controlSink: blocked, admission: admission) { peer in
            do {
                try await prepareStoppedCallback(
                    peer: peer,
                    runtime: runtime,
                    blocked: blocked,
                    diagnostics: diagnostics
                )
            } catch {
                diagnostics.report(peer: peer, blocked: blocked, admission: admission)
                throw error
            }
            try await verifyStoppedCallback(
                peer: peer,
                runtime: runtime,
                blocked: blocked,
                admission: admission,
                diagnostics: diagnostics
            )
        }
        let usage = await runtime.resourceUsage()
        XCTAssertEqual(usage.orphanedCallbacks, 0)
        XCTAssertEqual(usage.retainedBytes, 0)
    }

    private func prepareStoppedCallback(
        peer: Stage2HTTPContext,
        runtime: HTTPControlRuntime,
        blocked: Stage2BlockedControlSink,
        diagnostics: Stage2RestartDiagnostics
    ) async throws {
        diagnostics.begin("first-connect")
        let client = try release022WireClient(
            group: peer.group,
            port: peer.proxyPort,
            observation: diagnostics.firstWire
        )
        diagnostics.begin("first-write")
        try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string:
            "GET http://127.0.0.1:\(peer.origin.port)/blocked HTTP/1.1\r\n"
                + "Host: localhost\r\nX-Snapshot: retained\r\n\r\n")))
        diagnostics.begin("callback-entry")
        try await stage2Await { blocked.entered }
        diagnostics.begin("first-close")
        try await client.close().get()
        diagnostics.begin("orphan-registration")
        try await stage2AwaitAsync {
            let usage = await runtime.resourceUsage()
            diagnostics.usage = usage
            return usage.orphanedCallbacks == 1
        }
    }

    private func verifyStoppedCallback(
        peer: Stage2HTTPContext,
        runtime: HTTPControlRuntime,
        blocked: Stage2BlockedControlSink,
        admission: ResourceAdmissionController,
        diagnostics: Stage2RestartDiagnostics
    ) async throws {
        let stopping = NIOLockedValueBox(false)
        let proxy = peer.proxy
        let stop = Task {
            try await proxy.stop()
            stopping.withLockedValue { $0 = true }
        }
        do {
            diagnostics.begin("stop-completion")
            try await stage2Await {
                let completed = stopping.withLockedValue { $0 }
                diagnostics.stopCompleted = completed
                return completed
            }
            diagnostics.begin("stopped-usage")
            let orphan = await runtime.resourceUsage()
            diagnostics.usage = orphan
            XCTAssertEqual(orphan.orphanedCallbacks, 1)
            XCTAssertEqual(orphan.callbackTasks, 1)
            XCTAssertGreaterThan(orphan.retainedBytes, 0)
            XCTAssertGreaterThan(admission.usage.retainedBytes, 0)
            XCTAssertEqual(admission.usage.connections, 0)
            XCTAssertEqual(admission.usage.streams, 0)
            XCTAssertTrue(peer.origin.requests.isEmpty)
            diagnostics.begin("callback-release")
            blocked.release()
            diagnostics.callbackReleased = true
            diagnostics.begin("stop-join")
            try await stop.value
            diagnostics.begin("final-zero")
            try await stage2AwaitAsync {
                let usage = await runtime.resourceUsage()
                diagnostics.usage = usage
                return usage.callbackTasks == 0 && usage.retainedBytes == 0 && admission.usage.retainedBytes == 0
            }
        } catch {
            let failure = error
            diagnostics.report(peer: peer, blocked: blocked, admission: admission)
            blocked.release()
            do { try await stop.value } catch { XCTFail("Stop failed after callback release: \(error)") }
            throw failure
        }
    }

    func testRestartWithSharedEventLoopRetainsOrphanAccountingAndServesNewTraffic() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(id: "restart-orphan", match: HTTPRuleMatch(phase: .request, path: "/blocked"), action: .breakpoint)
        ], limits: HTTPControlLimits(maximumTasks: 2)))
        let blocked = Stage2BlockedControlSink()
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        let diagnostics = Stage2RestartDiagnostics()
        defer { blocked.release() }
        try await stage2WithHTTPOrigin(runtime: runtime, controlSink: blocked, admission: admission) { peer in
            do {
                try await prepareRestart(peer: peer, runtime: runtime, blocked: blocked, diagnostics: diagnostics)
                let retainedBeforeRestart = admission.usage.retainedBytes
                XCTAssertGreaterThan(retainedBeforeRestart, 0)
                diagnostics.begin("restart")
                let port = try await peer.proxy.start(port: 0)
                diagnostics.begin("restarted-usage")
                let restarted = await runtime.resourceUsage()
                diagnostics.usage = restarted
                XCTAssertEqual(restarted.orphanedCallbacks, 1)
                XCTAssertEqual(restarted.callbackTasks, 1)
                XCTAssertEqual(admission.usage.retainedBytes, retainedBeforeRestart)
                try await verifyRestartTraffic(peer: peer, port: port, diagnostics: diagnostics)
                diagnostics.begin("second-stop")
                try await peer.proxy.stop()
                XCTAssertGreaterThan(admission.usage.retainedBytes, 0)
                diagnostics.begin("callback-release")
                blocked.release()
                diagnostics.callbackReleased = true
                diagnostics.begin("final-zero")
                try await stage2AwaitAsync {
                    let usage = await runtime.resourceUsage()
                    diagnostics.usage = usage
                    return usage.callbackTasks == 0 && usage.retainedBytes == 0 && admission.usage.retainedBytes == 0
                }
            } catch {
                diagnostics.report(peer: peer, blocked: blocked, admission: admission)
                throw error
            }
        }
    }

    private func prepareRestart(
        peer: Stage2HTTPContext,
        runtime: HTTPControlRuntime,
        blocked: Stage2BlockedControlSink,
        diagnostics: Stage2RestartDiagnostics
    ) async throws {
        diagnostics.begin("first-connect")
        let first = try release022WireClient(
            group: peer.group,
            port: peer.proxyPort,
            observation: diagnostics.firstWire
        )
        diagnostics.begin("first-write")
        try phase4BoundedWait(first.writeAndFlush(ByteBuffer(string:
            "GET http://127.0.0.1:\(peer.origin.port)/blocked HTTP/1.1\r\nHost: localhost\r\n\r\n")))
        diagnostics.begin("callback-entry")
        try await stage2Await { blocked.entered }
        diagnostics.begin("first-close")
        try await first.close().get()
        diagnostics.begin("orphan-registration")
        try await stage2AwaitAsync {
            let usage = await runtime.resourceUsage()
            diagnostics.usage = usage
            return usage.orphanedCallbacks == 1
        }
        diagnostics.begin("first-stop")
        try await peer.proxy.stop()
    }

    private func verifyRestartTraffic(
        peer: Stage2HTTPContext,
        port: Int,
        diagnostics: Stage2RestartDiagnostics
    ) async throws {
        diagnostics.begin("healthy-connect")
        let received = diagnostics.healthyWire
        let next = try release022WireClient(group: peer.group, port: port, observation: received)
        diagnostics.begin("healthy-write")
        try phase4BoundedWait(next.writeAndFlush(ByteBuffer(string:
            "GET http://127.0.0.1:\(peer.origin.port)/healthy HTTP/1.1\r\nHost: localhost\r\n\r\n")))
        diagnostics.begin("healthy-response")
        _ = try received.wait { String(bytes: $0, encoding: .utf8)?.hasPrefix("HTTP/1.1 200 ") == true }
        XCTAssertEqual(peer.origin.requests.map(\.head.uri), ["/healthy"])
        diagnostics.begin("healthy-close")
        try await next.close().get()
    }
}

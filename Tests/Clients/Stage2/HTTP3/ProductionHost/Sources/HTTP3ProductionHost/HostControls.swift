import Dispatch
import Foundation
import NIOConcurrencyHelpers
import SwiftMITM

struct HostControlRecord: Encodable, Sendable {
    let kind: String
    let requestID: UUID
    var phase: String?
    var headers: [HostField]?
    var failure: String?
    var authority: String?
    var destinationAddress: String?
    var destinationPort: Int?
}

final class HostControlEvents: HTTPControlEventSink {
    private struct State: Sendable {
        var records: [HostControlRecord] = []
        var handles: [BreakpointHandle] = []
        var entered = 0
        var exited = 0
        var overflow = false
        var barrierExpired = false
    }

    private let state = NIOLockedValueBox(State())
    private let releaseSignal = DispatchSemaphore(value: 0)
    private let blockCallback: Bool

    init(blockCallback: Bool) { self.blockCallback = blockCallback }

    func receive(_ event: HTTPControlEvent) {
        let record: HostControlRecord
        switch event {
        case let .effectiveHead(id, phase, headers, _):
            var value = HostControlRecord(kind: "effectiveHead", requestID: id)
            value.phase = String(describing: phase)
            value.headers = headers.map(HostField.init)
            record = value
        case let .breakpoint(handle):
            record = HostControlRecord(kind: "breakpoint", requestID: handle.requestID)
            state.withLockedValue {
                if $0.handles.count < 16 {
                    $0.handles.append(handle)
                } else {
                    $0.overflow = true
                }
                $0.entered += 1
            }
        case let .failure(id, code):
            var value = HostControlRecord(kind: "failure", requestID: id)
            value.failure = String(describing: code)
            record = value
        case let .noTransformOverride(id, phase, _):
            var value = HostControlRecord(kind: "noTransformOverride", requestID: id)
            value.phase = String(describing: phase)
            record = value
        case let .connectedTarget(id, target):
            var value = HostControlRecord(kind: "connectedTarget", requestID: id)
            value.authority = target.logicalAuthority
            value.destinationAddress = target.destination.address
            value.destinationPort = target.destination.port
            record = value
        }
        state.withLockedValue {
            if $0.records.count < 4096 {
                $0.records.append(record)
            } else {
                $0.overflow = true
            }
        }
        if case .breakpoint = event, blockCallback {
            waitForCallbackRelease()
        }
    }

    private func waitForCallbackRelease() {
        // A real host callback deliberately ignores cancellation; the peer controls its bounded release barrier.
        let outcome = releaseSignal.wait(timeout: .now() + 45)
        state.withLockedValue {
            $0.exited += 1
            if outcome == .timedOut {
                $0.barrierExpired = true
            }
        }
    }

    func releaseBlocked() { releaseSignal.signal() }

    func resolvePending() async throws -> [String] {
        let handles = state.withLockedValue {
            let values = $0.handles
            $0.handles.removeAll(keepingCapacity: false)
            return values
        }
        var outcomes: [String] = []
        for handle in handles {
            let rule = HTTPRule(
                id: "peer-resume",
                match: HTTPRuleMatch(phase: handle.phase),
                action: .editHeaders(remove: [], append: [HTTPHeaderField(name: "x-resumed", value: "yes")])
            )
            let configuration = try HTTPControlConfiguration(rules: [rule], limits: HTTPControlLimits())
            let decision = try configuration.evaluate(handle.original)
            await outcomes.append(String(describing: handle.resolve(.decision(decision))))
        }
        return outcomes
    }

    func write(to path: String) throws {
        struct Snapshot: Encodable {
            let records: [HostControlRecord]
            let callbacksEntered: Int
            let callbacksExited: Int
        }
        let snapshot = try state.withLockedValue { state in
            guard !state.overflow, !state.barrierExpired else { throw HostError.captureOverflow }
            return Snapshot(
                records: state.records,
                callbacksEntered: state.entered,
                callbacksExited: state.exited
            )
        }
        try JSONEncoder()
            .encode(snapshot)
            .write(to: URL(fileURLWithPath: path), options: .atomic)
    }
}

actor HostControlCommands {
    private var lastID: String?
    let events: HostControlEvents
    let prefix: String

    init(events: HostControlEvents, prefix: String) {
        self.events = events
        self.prefix = prefix
    }

    func poll() async throws {
        struct Command: Decodable { let id: String; let action: String }
        let path = prefix + ".control-command.json"
        guard FileManager.default.fileExists(atPath: path) else { return }
        let command = try JSONDecoder().decode(Command.self, from: Data(contentsOf: URL(fileURLWithPath: path)))
        guard command.id != lastID else { return }
        let resolutions: [String]
        switch command.action {
        case "resolve": resolutions = try await events.resolvePending()
        case "releaseCallback":
            events.releaseBlocked()
            resolutions = []
        default: throw HostError.arguments
        }
        lastID = command.id
        try JSONSerialization.data(withJSONObject: ["id": command.id, "resolutions": resolutions])
            .write(to: URL(fileURLWithPath: prefix + ".control-ack.json"), options: .atomic)
    }
}

import Foundation

public enum HTTPControlEvent: Sendable {
    case effectiveHead(requestID: UUID, phase: HTTPControlPhase, headers: [HTTPHeaderField], appliedRuleIDs: [String])
    case breakpoint(BreakpointHandle)
    case failure(requestID: UUID, code: HTTPControlError)
    case noTransformOverride(requestID: UUID, phase: HTTPControlPhase, appliedRuleIDs: [String])
    case connectedTarget(requestID: UUID, target: CapturedTarget)
}

public protocol HTTPControlEventSink: Sendable {
    func receive(_ event: HTTPControlEvent)
}

enum HTTPControlTransport {
    static func resolve(
        _ decision: HTTPControlDecision,
        message: HTTPControlMessage,
        runtime: HTTPControlRuntime,
        sink: any HTTPControlEventSink?,
        snapshot: HTTPControlConfiguration? = nil
    ) async throws -> HTTPControlDecision {
        guard case .breakpoint = decision.action else { return decision }
        guard !Task.isCancelled else { throw HTTPControlError.cancelled }
        let proposed = HTTPControlDecision(
            headers: decision.headers,
            action: .forward,
            appliedRuleIDs: decision.appliedRuleIDs,
            faults: decision.faults,
            overridesNoTransform: decision.overridesNoTransform,
            syntheticResponseHeaders: decision.syntheticResponseHeaders
        )
        let policy = snapshot ?? runtime.configurationSnapshot
        let handle = try await runtime.pause(
            original: message,
            proposed: proposed,
            deadlineSeconds: policy.breakpointDeadlineSeconds,
            expiry: policy.breakpointExpiry
        )
        guard let sink else {
            _ = await handle.resolve(.reject)
            _ = await handle.wait()
            throw HTTPControlError.unsupportedEdit
        }
        do {
            guard !Task.isCancelled else { throw HTTPControlError.cancelled }
            let delivery = notify(
                handle: handle,
                proposed: proposed,
                message: message,
                runtime: runtime,
                sink: sink,
                snapshot: snapshot
            )
            let resolution = await handle.wait()
            delivery.cancel()
            if let (outcome, error) = await delivery.value, outcome == .resumed {
                throw error
            }
            switch resolution {
            case .forwardOriginal:
                let original = try HTTPControlConfiguration(rules: [], limits: runtime.configurationSnapshot.limits)
                return try original.evaluate(message)
            case let .decision(edited): return edited
            case .reject: throw HTTPControlError.deadlineExceeded
            case .cancelled: throw HTTPControlError.cancelled
            }
        } catch {
            _ = await handle.resolve(.cancelled)
            _ = await handle.wait()
            throw error
        }
    }

    private static func notify(
        handle: BreakpointHandle,
        proposed: HTTPControlDecision,
        message: HTTPControlMessage,
        runtime: HTTPControlRuntime,
        sink: any HTTPControlEventSink,
        snapshot: HTTPControlConfiguration?
    ) -> Task<(HTTPBreakpointResumeResult, HTTPControlError)?, Never> {
        Task {
            do {
                _ = try await runtime.runCallback(
                    message: message,
                    retainedSnapshot: snapshot,
                    retainedDecision: proposed,
                    owningBreakpointID: handle.id
                ) { _ in
                    sink.receive(.breakpoint(handle))
                    return proposed
                }
                return nil
            } catch {
                let outcome = await handle.resolve(.reject)
                return (outcome, error as? HTTPControlError ?? .cancelled)
            }
        }
    }
}

import Foundation
import NIOCore
import SwiftMITM

extension HTTP3Exchange {
    func controlMessage(_ flow: HTTP3Flow, body: Data? = nil) throws -> HTTPControlMessage {
        guard let context, let snapshot = flow.snapshot else { throw HTTP3ProxyError.protocolViolation }
        var status: Int?
        if case let .response(.head(head)) = flow.head {
            status = head.status.code
        }
        return HTTPControlMessage(
            context: context,
            phase: flow.phase,
            status: status,
            headers: snapshot.fields,
            body: body
        )
    }

    func evaluateHead(_ flow: HTTP3Flow) throws {
        guard let configuration else { try forwardHead(flow); return }
        let message = try controlMessage(flow)
        switch try configuration.evaluateHead(message) {
        case let .decision(decision): try resolve(decision, message: message, flow: flow)
        case let .needsBody(maximum):
            let preparation = try configuration.evaluate(controlMessage(flow, body: Data()))
            try retainDecision(preparation, flow: flow)
            flow.bodyMaximum = maximum
            flow.body = Data()
            flow.body?.reserveCapacity(maximum)
        }
    }

    func retainDecision(_ decision: HTTPControlDecision, flow: HTTP3Flow) throws {
        guard let runtime = transport.controlRuntime else { throw HTTPControlError.invalidConfiguration }
        let work = try HTTPControlWorkLifetime(
            decision: decision,
            runtime: runtime,
            admission: transport.localAdmission,
            transportAdmission: transport.sharedAdmission
        )
        flow.work = work
        flow.decision = decision
    }

    func resolve(_ decision: HTTPControlDecision, message: HTTPControlMessage, flow: HTTP3Flow) throws {
        try retainDecision(decision, flow: flow)
        guard case .breakpoint = decision.action else { try apply(decision, message: message, flow: flow); return }
        guard let runtime = transport.controlRuntime else { throw HTTPControlError.invalidConfiguration }
        flow.waitingForControl = true
        controlTasks += 1
        let bound = NIOLoopBound((self, flow), eventLoop: transport.eventLoop)
        let loop = transport.eventLoop
        let sink = transport.controlEventSink
        let snapshot = configuration
        let lifetime = flow.work
        let snapshotLifetime = configurationWork
        let localAdmission = transport.localAdmission
        let sharedAdmission = transport.sharedAdmission
        flow.task = Task {
            let result: Result<(decision: HTTPControlDecision, work: HTTPControlWorkLifetime), Error>
            do {
                let resolved = try await HTTPControlTransport.resolve(
                    decision,
                    message: message,
                    runtime: runtime,
                    sink: sink,
                    snapshot: snapshot
                )
                try snapshot?.validate(resolved, for: message)
                let work = try HTTPControlWorkLifetime(
                    decision: resolved,
                    runtime: runtime,
                    admission: localAdmission,
                    transportAdmission: sharedAdmission
                )
                result = .success((resolved, work))
            } catch { result = .failure(error) }
            loop.execute {
                withExtendedLifetime((lifetime, snapshotLifetime)) {
                    let (exchange, direction) = bound.value
                    exchange.resolved(result, message: message, flow: direction)
                }
            }
        }
    }

    func resolved(
        _ result: Result<(decision: HTTPControlDecision, work: HTTPControlWorkLifetime), Error>,
        message: HTTPControlMessage,
        flow: HTTP3Flow
    ) {
        defer {
            let bound = NIOLoopBound(self, eventLoop: transport.eventLoop)
            transport.eventLoop.execute {
                bound.value.controlTasks -= 1
                bound.value.finishIfReady()
            }
        }
        flow.task = nil
        flow.waitingForControl = false
        guard !failed, !finished else { return }
        do {
            let (decision, work) = try result.get()
            try configuration?.validate(decision, for: message)
            flow.work = work
            flow.decision = decision
            try apply(decision, message: message, flow: flow)
            while !flow.input.isEmpty, !failed {
                let held = flow.input.removeFirst()
                flow.pendingBytes -= held.message.body?.readableBytes ?? 0
                try withExtendedLifetime(held.lease) { try consume(held.message, flow: flow) }
            }
            observePending()
            readIfReady(flow)
        } catch { fail(error) }
    }

    func apply(_ decision: HTTPControlDecision, message: HTTPControlMessage, flow: HTTP3Flow) throws {
        try configuration?.validate(decision, for: message)
        if let fault = try decision.selectedFault(direction: flow.direction) {
            flow.fault = HTTPFaultSchedule(fault: fault, startNanoseconds: NIODeadline.now().uptimeNanoseconds)
        }
        switch decision.action {
        case .abort: throw HTTPControlError.cancelled
        case .breakpoint: throw HTTPControlError.unsupportedEdit
        case let .replaceBody(_, maximum, _, _):
            if message.body == nil {
                flow.bodyMaximum = maximum
                flow.body = Data()
                flow.body?.reserveCapacity(maximum)
                return
            }
            try forwardHead(flow)
        case let .respond(status, _, body):
            try syntheticResponse(status: status, body: body, decision: decision, flow: flow)
        default: try forwardHead(flow)
        }
        if decision.overridesNoTransform {
            transport.controlEventSink?.receive(.noTransformOverride(
                requestID: id,
                phase: flow.phase,
                appliedRuleIDs: decision.appliedRuleIDs
            ))
        }
    }

    func completeCollectedBody(_ end: HTTP3Message, flow: HTTP3Flow) throws {
        guard let configuration, let body = flow.body, let decision = flow.decision else {
            throw HTTPControlError.invalidConfiguration
        }
        let message = try controlMessage(flow, body: body)
        try configuration.validate(decision, for: message)
        flow.body = nil
        try apply(decision, message: message, flow: flow)
        guard case let .replaceBody(replacement, _, _, _) = decision.action else {
            throw HTTPControlError.invalidConfiguration
        }
        flow.generatedBody = replacement
        flow.generatedOffset = 0
        flow.generatedEnd = try effectiveEnd(end, flow: flow)
        pump(flow)
    }
}

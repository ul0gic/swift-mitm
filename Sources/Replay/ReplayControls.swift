import Foundation
import NIOCore

final class ReplayControls: Sendable {
    let runtime: HTTPControlRuntime
    let snapshot: HTTPControlConfiguration
    let sink: any HTTPControlEventSink?
    let requestID: UUID
    let original: HTTPControlMessage
    private let reservation: HTTPControlByteReservation

    init(
        runtime: HTTPControlRuntime,
        sink: any HTTPControlEventSink?,
        requestID: UUID,
        request: ReplayRequest,
        target: HTTPForwardTarget
    ) throws {
        self.runtime = runtime
        let snapshot = runtime.configurationSnapshot
        self.snapshot = snapshot
        self.sink = sink
        self.requestID = requestID
        let metadata = request.headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
        let retained = snapshot.retainedBytes + metadata + request.body.count
            + snapshot.limits.maximumBodyBytes + 65_536
        reservation = try runtime.reserveTransportBytes(retained)
        let context = HTTPControlContext(
            requestID: requestID,
            authority: target.authority,
            method: request.method,
            path: target.requestTarget
        )
        original = HTTPControlMessage(
            context: context,
            phase: .request,
            headers: request.headers,
            body: Data(request.body)
        )
    }

    func requestDecision() async throws -> HTTPControlDecision {
        let decision: HTTPControlDecision
        switch try snapshot.evaluateHead(original) {
        case let .decision(value): decision = value
        case let .needsBody(maximum):
            guard (original.body?.count ?? 0) <= maximum else { throw HTTPControlError.bodyLimitExceeded }
            decision = try snapshot.evaluate(original)
        }
        return try await resolve(decision, message: original)
    }

    func resolve(_ decision: HTTPControlDecision, message: HTTPControlMessage) async throws -> HTTPControlDecision {
        let result = try await HTTPControlTransport.resolve(
            decision,
            message: message,
            runtime: runtime,
            sink: sink,
            snapshot: snapshot
        )
        try validate(result, message: message)
        sink?.receive(.effectiveHead(
            requestID: requestID,
            phase: message.phase,
            headers: result.headers,
            appliedRuleIDs: result.appliedRuleIDs
        ))
        if result.overridesNoTransform {
            sink?.receive(.noTransformOverride(
                requestID: requestID,
                phase: message.phase,
                appliedRuleIDs: result.appliedRuleIDs
            ))
        }
        return result
    }

    func validate(_ decision: HTTPControlDecision, message: HTTPControlMessage) throws {
        _ = try HTTPControlConfiguration.headerBytes(decision.headers, multiplexed: false)
        if let synthetic = decision.syntheticResponseHeaders {
            _ = try HTTPControlConfiguration.headerBytes(synthetic, multiplexed: false)
        }
        for fault in decision.faults { try fault.validate() }
        switch decision.action {
        case let .replaceBody(body, maximum, _, override):
            try validateReplacement(body, maximum: maximum, override: override, message: message)
        case let .respond(status, _, body):
            guard (200...599).contains(status), body.count <= snapshot.limits.maximumBodyBytes,
                  ![204, 205, 304].contains(status) || body.isEmpty,
                  message.method != "HEAD" || body.isEmpty else { throw HTTPControlError.unsupportedEdit }
        default: break
        }
    }

    private func validateReplacement(
        _ body: Data,
        maximum: Int,
        override: Bool,
        message: HTTPControlMessage
    ) throws {
        guard !HTTPControlConfiguration.unsupported(message),
              (1...snapshot.limits.maximumBodyBytes).contains(maximum),
              body.count <= snapshot.limits.maximumBodyBytes else { throw HTTPControlError.unsupportedEdit }
        if let input = message.body, input.count > maximum {
            throw HTTPControlError.bodyLimitExceeded
        }
        guard override || !message.headers.contains(where: {
            $0.name.lowercased() == "cache-control" && $0.value.lowercased().split(separator: ",").contains(where: {
                $0.trimmingCharacters(in: .whitespaces) == "no-transform"
            })
        }) else { throw HTTPControlError.noTransform }
    }

    func responseMessage(_ head: CapturedResponseHead, body: Data? = nil) -> HTTPControlMessage {
        HTTPControlMessage(
            context: original.context,
            phase: .response,
            status: head.status,
            headers: head.headers,
            body: body
        )
    }
}

struct ReplayFaultPacer: Sendable {
    private var schedules: [HTTPFaultSchedule]

    init(_ faults: [HTTPFault], direction: HTTPFaultDirection) {
        let now = NIODeadline.now().uptimeNanoseconds
        schedules = faults.filter { $0.direction == direction }
            .map { HTTPFaultSchedule(fault: $0, startNanoseconds: now) }
    }

    mutating func beforeHead() async throws {
        while true {
            try Task.checkCancellation()
            let now = NIODeadline.now().uptimeNanoseconds
            var allowances: [HTTPFaultAllowance] = []
            for index in schedules.indices { allowances.append(schedules[index].next(nowNanoseconds: now)) }
            guard !allowances.contains(where: { $0.reset != nil }) else { throw HTTPControlError.cancelled }
            let wait = allowances.map(\.waitNanoseconds).max() ?? 0
            if wait == 0 {
                return
            }
            try await Task.sleep(nanoseconds: wait)
        }
    }

    mutating func send(_ bytes: [UInt8], emit: ([UInt8]) async throws -> Void) async throws {
        guard !schedules.isEmpty else { try await emit(bytes); return }
        for index in schedules.indices { try schedules[index].enqueue(bytes: bytes.count) }
        var offset = 0
        while offset < bytes.count {
            try await beforeHead()
            let now = NIODeadline.now().uptimeNanoseconds
            var count = bytes.count - offset
            for index in schedules.indices { count = min(count, schedules[index].next(nowNanoseconds: now).bytes) }
            guard count > 0 else { throw HTTPControlError.invalidConfiguration }
            try await emit(Array(bytes[offset..<(offset + count)]))
            for index in schedules.indices {
                try schedules[index].didForward(bytes: count, nowNanoseconds: NIODeadline.now().uptimeNanoseconds)
            }
            offset += count
        }
    }
}

struct ReplayResponseControls: Sendable {
    let controls: ReplayControls?
    let inheritedFaults: [HTTPFault]
    let attemptID: UUID
    let responseSink: any ReplayResponseSink
    let observation: ObservationCaptureSink?
    private var originalHead: CapturedResponseHead?
    private var decision: HTTPControlDecision?
    private var collected: Data?
    private var maximum = 0
    private var reservation: HTTPControlByteReservation?
    private var pacer = ReplayFaultPacer([], direction: .response)
    private(set) var effectiveStatus: Int?
    private(set) var effectiveBodyBytes = 0
    private(set) var effectiveHeaders: [HTTPHeaderField] = []

    var representationChanged: Bool {
        guard let decision else { return false }
        switch decision.action {
        case .respond, .replaceBody: return true
        default: return false
        }
    }

    var completesAtHead: Bool {
        guard let decision else { return false }
        if case .respond = decision.action {
            return true
        }
        return false
    }

    init(
        controls: ReplayControls?,
        inheritedFaults: [HTTPFault],
        attemptID: UUID,
        responseSink: any ReplayResponseSink,
        observation: ObservationCaptureSink? = nil
    ) {
        self.controls = controls
        self.inheritedFaults = inheritedFaults
        self.attemptID = attemptID
        self.responseSink = responseSink
        self.observation = observation
    }

    mutating func head(_ head: CapturedResponseHead) async throws {
        if head.status < 200 {
            try await responseSink.receive(.head(attemptID: attemptID, head: head))
            return
        }
        originalHead = head
        guard let controls else {
            effectiveStatus = head.status
            effectiveHeaders = head.headers
            try await responseSink.receive(.head(attemptID: attemptID, head: head))
            return
        }
        let message = controls.responseMessage(head)
        switch try controls.snapshot.evaluateHead(message) {
        case let .needsBody(maximum):
            try collect(maximum: maximum, decision: nil, controls: controls)
        case let .decision(decision):
            let resolved = try await controls.resolve(decision, message: message)
            if case let .replaceBody(_, maximum, _, _) = resolved.action {
                try collect(maximum: maximum, decision: resolved, controls: controls)
            } else {
                try await apply(resolved, head: head)
            }
        }
    }

    private mutating func collect(
        maximum: Int,
        decision: HTTPControlDecision?,
        controls: ReplayControls
    ) throws {
        self.maximum = maximum
        reservation = try controls.runtime.reserveTransportBytes(maximum + controls.snapshot.limits.maximumBodyBytes)
        self.decision = decision
        collected = Data()
    }

    mutating func body(_ bytes: [UInt8], requestID: UUID) async throws {
        if collected != nil {
            guard bytes.count <= maximum - (collected?.count ?? 0) else { throw HTTPControlError.bodyLimitExceeded }
            collected?.append(contentsOf: bytes)
            return
        }
        if let decision {
            switch decision.action {
            case .respond, .replaceBody: return
            default: break
            }
        }
        let sink = responseSink
        let id = attemptID
        let observer = controls == nil ? nil : observation
        try await pacer.send(bytes) {
            observer?.receiveBody(requestID: requestID, direction: .response, representation: .effective, bytes: $0)
            try await sink.receive(.body(attemptID: id, requestID: requestID, bytes: $0))
        }
        effectiveBodyBytes += bytes.count
    }

    mutating func end() async throws {
        guard let controls, let originalHead, let collected else {
            try await pacer.beforeHead()
            if controls != nil, let originalHead {
                observation?.receiveEffective(.responseEnd(requestID: originalHead.requestID, truncated: false))
            }
            return
        }
        let message = controls.responseMessage(originalHead, body: collected)
        let resolved: HTTPControlDecision
        if let decision {
            try controls.validate(decision, message: message)
            resolved = decision
        } else {
            let value = try controls.snapshot.evaluate(message)
            resolved = try await controls.resolve(value, message: message)
        }
        try await apply(resolved, head: originalHead)
        observation?.receiveEffective(.responseEnd(requestID: originalHead.requestID, truncated: false))
        self.collected = nil
        reservation?.release()
        reservation = nil
    }

    private mutating func apply(_ decision: HTTPControlDecision, head: CapturedResponseHead) async throws {
        self.decision = decision
        pacer = ReplayFaultPacer(inheritedFaults + decision.faults, direction: .response)
        try await pacer.beforeHead()
        var status = head.status
        let replacement: Data?
        switch decision.action {
        case let .respond(code, _, body):
            status = code
            replacement = body
        case let .replaceBody(body, _, _, _): replacement = body
        case .abort: throw HTTPControlError.cancelled
        case .route, .breakpoint: throw HTTPControlError.unsupportedEdit
        case .forward, .editHeaders, .fault: replacement = nil
        }
        effectiveStatus = status
        effectiveHeaders = decision.headers
        let effective = CapturedResponseHead(
            requestID: head.requestID,
            timestamp: head.timestamp,
            status: status,
            version: head.version,
            headers: decision.headers
        )
        observation?.receiveEffective(.responseHead(effective))
        try await responseSink.receive(.head(attemptID: attemptID, head: effective))
        if let replacement {
            try await sendReplacement(replacement, requestID: head.requestID)
            effectiveBodyBytes = replacement.count
        }
    }

    private mutating func sendReplacement(_ replacement: Data, requestID: UUID) async throws {
        let sink = responseSink
        let id = attemptID
        let observer = observation
        var offset = 0
        while offset < replacement.count {
            let end = min(offset + 65_536, replacement.count)
            try await pacer.send(Array(replacement[offset..<end])) {
                observer?.receiveBody(
                    requestID: requestID,
                    direction: .response,
                    representation: .effective,
                    bytes: $0
                )
                try await sink.receive(.body(attemptID: id, requestID: requestID, bytes: $0))
            }
            offset = end
        }
    }
}

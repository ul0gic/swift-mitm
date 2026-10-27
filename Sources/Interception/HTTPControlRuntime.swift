import Foundation

public enum HTTPBreakpointResolution: Sendable {
    case forwardOriginal
    case decision(HTTPControlDecision)
    case reject
    case cancelled
}

public enum HTTPBreakpointResumeResult: Sendable { case resumed, alreadyCompleted }
public enum HTTPBreakpointExpiry: Sendable { case reject, forwardOriginal }

public struct HTTPControlStopReport: Sendable {
    public let abandonedCallbacks: Int
    public let retainedCallbackBytes: Int
}

public struct HTTPControlResourceUsage: Sendable {
    public let pausedExchanges: Int
    public let callbackTasks: Int
    public let orphanedCallbacks: Int
    public let retainedBytes: Int
    public let taskSlots: Int
}

public struct BreakpointHandle: Sendable {
    public let id: UUID
    public let requestID: UUID
    public let phase: HTTPControlPhase
    public let original: HTTPControlMessage
    public let proposed: HTTPControlDecision
    private let runtime: HTTPControlRuntime
    private let completion: HTTPBreakpointCompletion

    init(
        id: UUID,
        original: HTTPControlMessage,
        proposed: HTTPControlDecision,
        runtime: HTTPControlRuntime,
        completion: HTTPBreakpointCompletion
    ) {
        self.id = id
        requestID = original.requestID
        phase = original.phase
        self.original = original
        self.proposed = proposed
        self.runtime = runtime
        self.completion = completion
    }

    public func wait() async -> HTTPBreakpointResolution {
        await withTaskCancellationHandler {
            await completion.wait()
        } onCancel: {
            Task { await runtime.resolve(id: id, resolution: .cancelled) }
        }
    }

    public func resolve(_ resolution: HTTPBreakpointResolution) async -> HTTPBreakpointResumeResult {
        await runtime.resolve(id: id, resolution: resolution)
    }
}

public actor HTTPControlRuntime {
    private struct Hold {
        let requestID: UUID
        let phase: HTTPControlPhase
        let bytes: Int
        let globalReservations: [ResourceReservation]
        let timer: Task<Void, Never>
        let completion: HTTPBreakpointCompletion
    }

    private struct Callback {
        let requestID: UUID
        let breakpointID: UUID?
        let bytes: Int
        let globalReservations: [ResourceReservation]
        let task: Task<Void, Never>
        let timer: Task<Void, Never>
        var waiter: CheckedContinuation<HTTPControlDecision, any Error>?
    }

    private struct ByteReservation {
        let requestID: UUID
        let bytes: Int
        let global: [ResourceReservation]
    }

    private var configuration: HTTPControlConfiguration
    private var holds: [UUID: Hold] = [:]
    private var callbacks: [UUID: Callback] = [:]
    private var byteReservations: [UUID: ByteReservation] = [:]
    nonisolated private let budget: HTTPControlBudget
    private var retainedBytes: Int { budget.state.withLockedValue { $0.bytes } }
    private var stopped = false
    private var activeOwner: UUID?
    private var hasAdmissionBinding = false
    private var admissionController: ResourceAdmissionController?

    public init(configuration: HTTPControlConfiguration) {
        self.configuration = configuration
        budget = HTTPControlBudget(configuration: configuration)
    }

    func beginServing(
        owner: UUID,
        admission: ResourceAdmissionController?
    ) throws {
        guard activeOwner == nil || activeOwner == owner else { throw HTTPControlError.invalidConfiguration }
        let sameAdmission = admissionController === admission
        let hasActorWork = !holds.isEmpty || !callbacks.isEmpty || !byteReservations.isEmpty
        try budget.state.withLockedValue { state in
            let hasWork = state.bytes > 0 || hasActorWork
            guard !hasAdmissionBinding || sameAdmission || (stopped && !hasWork),
                  hasAdmissionBinding || sameAdmission || !hasWork else {
                throw HTTPControlError.invalidConfiguration
            }
            admissionController = admission
            hasAdmissionBinding = true
            activeOwner = owner
            stopped = false
            state.stopped = false
        }
    }

    func endServing(owner: UUID) -> HTTPControlStopReport? {
        guard activeOwner == owner else { return nil }
        activeOwner = nil
        return stop()
    }

    nonisolated public var configurationSnapshot: HTTPControlConfiguration {
        budget.state.withLockedValue { $0.configuration }
    }

    nonisolated public func reserveTransportBytes(_ bytes: Int) throws -> HTTPControlByteReservation {
        try budget.state.withLockedValue { value in
            guard !value.stopped else { throw HTTPControlError.cancelled }
            guard bytes > 0, value.reservations < value.configuration.limits.maximumTasks * 4,
                  bytes <= value.configuration.limits.maximumRetainedBytes - value.bytes else {
                throw HTTPControlError.capacityExceeded
            }
            value.bytes += bytes
            value.reservations += 1
        }
        return HTTPControlByteReservation { [budget] in
            budget.state.withLockedValue {
                $0.bytes -= bytes
                $0.reservations -= 1
            }
        }
    }

    public func snapshot() -> HTTPControlConfiguration { configuration }

    public func resourceUsage() -> HTTPControlResourceUsage {
        HTTPControlResourceUsage(
            pausedExchanges: holds.count,
            callbackTasks: callbacks.count,
            orphanedCallbacks: callbacks.values.filter { $0.waiter == nil }.count,
            retainedBytes: retainedBytes,
            taskSlots: taskSlots
        )
    }

    private var taskSlots: Int {
        holds.count + callbacks.values
            .filter { callback in
                guard let id = callback.breakpointID else { return true }
                return holds[id] == nil
            }
            .count
    }

    public func replaceConfiguration(_ replacement: HTTPControlConfiguration) throws {
        guard !stopped, taskSlots <= replacement.limits.maximumTasks else {
            throw HTTPControlError.capacityExceeded
        }
        try budget.state.withLockedValue {
            guard $0.bytes <= replacement.limits.maximumRetainedBytes,
                  $0.reservations <= replacement.limits.maximumTasks * 4 else {
                throw HTTPControlError.capacityExceeded
            }
            $0.configuration = replacement
        }
        configuration = replacement
    }

    private func admit(bytes: Int, sharingHold: UUID? = nil) throws {
        guard !stopped else { throw HTTPControlError.cancelled }
        guard sharingHold != nil || taskSlots < configuration.limits.maximumTasks,
              bytes <= configuration.limits.maximumRetainedBytes - retainedBytes else {
            throw HTTPControlError.capacityExceeded
        }
        try budget.charge(bytes)
    }

    private func reserveGlobalBytes(_ bytes: Int) throws -> [ResourceReservation] {
        var committed = false
        defer {
            if !committed {
                budget.release(bytes)
            }
        }
        do {
            let reservations = try HTTPControlWorkLifetime.reserveAdmission(
                bytes,
                admission: admissionController
            )
            committed = true
            return reservations
        } catch ResourceAdmissionError.capacityExceeded {
            throw HTTPControlError.capacityExceeded
        }
    }

    public func reserveBytes(requestID: UUID, bytes: Int) throws -> UUID {
        guard !stopped, bytes > 0,
              byteReservations.count < configuration.limits.maximumTasks,
              bytes <= configuration.limits.maximumRetainedBytes - retainedBytes else {
            throw HTTPControlError.capacityExceeded
        }
        let id = UUID()
        try budget.charge(bytes)
        let global = try reserveGlobalBytes(bytes)
        byteReservations[id] = ByteReservation(requestID: requestID, bytes: bytes, global: global)
        return id
    }

    public func releaseBytes(reservationID: UUID) {
        if let reservation = byteReservations.removeValue(forKey: reservationID) {
            budget.release(reservation.bytes)
        }
    }

    public func pause(
        original: HTTPControlMessage,
        proposed: HTTPControlDecision,
        deadlineSeconds: Int = 30,
        expiry: HTTPBreakpointExpiry = .reject
    ) throws -> BreakpointHandle {
        guard !Task.isCancelled else { throw HTTPControlError.cancelled }
        guard (1...300).contains(deadlineSeconds),
              (original.body?.count ?? 0) <= configuration.limits.maximumBodyBytes else {
            throw HTTPControlError.invalidConfiguration
        }
        let bytes = original.retainedBytes + proposed.retainedBytes + configuration.retainedBytes
        try admit(bytes: bytes)
        let global = try reserveGlobalBytes(bytes)
        let id = UUID()
        let timer = Task {
            do { try await Task.sleep(for: .seconds(deadlineSeconds)) } catch { return }
            self.expire(id: id, expiry: expiry)
        }
        let completion = HTTPBreakpointCompletion()
        holds[id] = Hold(
            requestID: original.requestID,
            phase: original.phase,
            bytes: bytes,
            globalReservations: global,
            timer: timer,
            completion: completion
        )
        return BreakpointHandle(
            id: id,
            original: original,
            proposed: proposed,
            runtime: self,
            completion: completion
        )
    }

    fileprivate func resolve(id: UUID, resolution: HTTPBreakpointResolution) -> HTTPBreakpointResumeResult {
        guard let hold = holds.removeValue(forKey: id) else { return .alreadyCompleted }
        hold.timer.cancel()
        budget.release(hold.bytes)
        hold.completion.complete(resolution)
        return .resumed
    }

    private func expire(id: UUID, expiry: HTTPBreakpointExpiry) {
        _ = resolve(id: id, resolution: expiry == .reject ? .reject : .forwardOriginal)
    }

    public func runCallback(
        message: HTTPControlMessage,
        retainedSnapshot: HTTPControlConfiguration? = nil,
        retainedDecision: HTTPControlDecision? = nil,
        owningBreakpointID: UUID? = nil,
        handler: @escaping @Sendable (HTTPControlMessage) async throws -> HTTPControlDecision
    ) async throws -> HTTPControlDecision {
        guard !Task.isCancelled else { throw HTTPControlError.cancelled }
        guard (message.body?.count ?? 0) <= configuration.limits.maximumBodyBytes else {
            throw HTTPControlError.bodyLimitExceeded
        }
        let bytes = message.retainedBytes + (retainedSnapshot ?? configuration).retainedBytes
            + (retainedDecision.map(\.retainedBytes) ?? 0)
        let id = UUID()
        if let owningBreakpointID {
            guard holds[owningBreakpointID]?.requestID == message.requestID,
                  holds[owningBreakpointID]?.phase == message.phase,
                  !callbacks.values.contains(where: { $0.breakpointID == owningBreakpointID }) else {
                throw HTTPControlError.cancelled
            }
        }
        try admit(bytes: bytes, sharingHold: owningBreakpointID)
        let global = try reserveGlobalBytes(bytes)
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let task = Task.detached {
                    let result: Result<HTTPControlDecision, any Error>
                    do { result = await .success(try handler(message)) } catch { result = .failure(error) }
                    await self.finishCallback(id: id, result: result)
                }
                let timer = Task {
                    do { try await Task.sleep(for: .seconds(5)) } catch { return }
                    self.cancelCallback(id: id, error: .deadlineExceeded)
                }
                callbacks[id] = Callback(
                    requestID: message.requestID,
                    breakpointID: owningBreakpointID,
                    bytes: bytes,
                    globalReservations: global,
                    task: task,
                    timer: timer,
                    waiter: continuation
                )
                if Task.isCancelled {
                    cancelCallback(id: id, error: .cancelled)
                }
            }
        } onCancel: {
            Task { await self.cancelCallback(id: id, error: .cancelled) }
        }
    }

    private func finishCallback(id: UUID, result: Result<HTTPControlDecision, any Error>) {
        guard let callback = callbacks.removeValue(forKey: id) else { return }
        callback.timer.cancel()
        budget.release(callback.bytes)
        callback.waiter?.resume(with: result)
    }

    private func cancelCallback(id: UUID, error: HTTPControlError) {
        guard var callback = callbacks[id] else { return }
        callback.task.cancel()
        callback.timer.cancel()
        let waiter = callback.waiter
        callback.waiter = nil
        callbacks[id] = callback
        waiter?.resume(throwing: error)
    }

    public func cancel(requestID: UUID) {
        for id in holds.keys.filter({ holds[$0]?.requestID == requestID }) {
            _ = resolve(id: id, resolution: .cancelled)
        }
        for id in callbacks.keys.filter({ callbacks[$0]?.requestID == requestID }) {
            cancelCallback(id: id, error: .cancelled)
        }
        for id in byteReservations.keys.filter({ byteReservations[$0]?.requestID == requestID }) {
            releaseBytes(reservationID: id)
        }
    }

    public func stop() -> HTTPControlStopReport {
        stopped = true
        budget.state.withLockedValue { $0.stopped = true }
        for id in Array(holds.keys) { _ = resolve(id: id, resolution: .cancelled) }
        for id in Array(callbacks.keys) { cancelCallback(id: id, error: .cancelled) }
        for id in Array(byteReservations.keys) { releaseBytes(reservationID: id) }
        return HTTPControlStopReport(
            abandonedCallbacks: callbacks.count,
            retainedCallbackBytes: callbacks.values.reduce(0) { $0 + $1.bytes }
        )
    }
}

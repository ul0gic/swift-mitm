import Foundation
import NIOConcurrencyHelpers
import NIOCore

final class ProxyConnectionSetupToken: Sendable {
    fileprivate struct ScheduledCancellation: Sendable {
        let eventLoop: EventLoop
        let callback: @Sendable () -> Void

        func schedule() {
            eventLoop.execute(callback)
        }
    }

    private enum Phase {
        case pending
        case cancelled
        case completed
    }

    private struct State {
        var phase = Phase.pending
        var cancelled = false
        var completionRequested = false
        var pendingOperations = 0
        var cancellations: [ScheduledCancellation] = []
        var terminalHandler: (@Sendable (UUID) -> Void)?
    }

    private struct CancellationTransition {
        let cancelled: Bool
        let cancellations: [ScheduledCancellation]
    }

    private let identifier: UUID
    private let state: NIOLockedValueBox<State>

    fileprivate init(identifier: UUID, terminalHandler: @escaping @Sendable (UUID) -> Void) {
        self.identifier = identifier
        state = NIOLockedValueBox(State(terminalHandler: terminalHandler))
    }

    var isCancelled: Bool {
        state.withLockedValue { $0.cancelled }
    }

    func onCancellation(on eventLoop: EventLoop, _ callback: @escaping @Sendable () -> Void) {
        let cancellation = ScheduledCancellation(eventLoop: eventLoop, callback: callback)
        let scheduleImmediately = state.withLockedValue { state in
            switch state.phase {
            case .pending:
                state.cancellations.append(cancellation)
                return false
            case .cancelled:
                return true
            case .completed:
                return false
            }
        }
        if scheduleImmediately {
            cancellation.schedule()
        }
    }

    @discardableResult
    func complete() -> Bool {
        let result = state.withLockedValue { state -> (Bool, (@Sendable (UUID) -> Void)?) in
            guard !state.completionRequested else { return (false, nil) }
            state.completionRequested = true
            return (!state.cancelled, Self.finishIfReady(&state))
        }
        result.1?(identifier)
        return result.0
    }

    func retainOperation() {
        state.withLockedValue { state in
            precondition(state.phase != .completed)
            state.pendingOperations += 1
        }
    }

    func releaseOperation() {
        let terminalHandler = state.withLockedValue { state in
            precondition(state.pendingOperations > 0)
            state.pendingOperations -= 1
            return Self.finishIfReady(&state)
        }
        terminalHandler?(identifier)
    }

    private static func finishIfReady(_ state: inout State) -> (@Sendable (UUID) -> Void)? {
        guard state.completionRequested, state.pendingOperations == 0 else { return nil }
        state.phase = .completed
        state.cancellations.removeAll()
        defer { state.terminalHandler = nil }
        return state.terminalHandler
    }

    @discardableResult
    func cancel() -> Bool {
        let transition = cancelTransition()
        guard transition.cancelled else { return false }
        transition.cancellations.forEach { $0.schedule() }
        return true
    }

    fileprivate func cancelFromRegistry() -> [ScheduledCancellation] {
        let transition = cancelTransition()
        return transition.cancelled ? transition.cancellations : []
    }

    private func cancelTransition() -> CancellationTransition {
        state.withLockedValue { state in
            guard state.phase == .pending else {
                return CancellationTransition(cancelled: false, cancellations: [])
            }
            state.phase = .cancelled
            state.cancelled = true
            let cancellations = state.cancellations
            state.cancellations.removeAll()
            return CancellationTransition(
                cancelled: true,
                cancellations: cancellations
            )
        }
    }
}

final class ProxyConnectionRegistry: Sendable {
    private enum Phase {
        case idle
        case accepting
        case shuttingDown
    }

    private struct State {
        var phase = Phase.idle
        var channels: [ObjectIdentifier: Channel] = [:]
        var setupTokens: [UUID: ProxyConnectionSetupToken] = [:]
        var waiters: [CheckedContinuation<Void, Never>] = []

        var isQuiescent: Bool {
            channels.isEmpty && setupTokens.isEmpty
        }
    }

    private struct ShutdownResources {
        let channels: [Channel]
        let cancellations: [ProxyConnectionSetupToken.ScheduledCancellation]
        let waiters: [CheckedContinuation<Void, Never>]
    }

    private let state = NIOLockedValueBox(State())

    func startAccepting() {
        state.withLockedValue { state in
            precondition(state.isQuiescent)
            precondition(state.waiters.isEmpty)
            state.phase = .accepting
        }
    }

    @discardableResult
    func register(_ channel: Channel) -> Bool {
        let identifier = ObjectIdentifier(channel)
        let accepted = state.withLockedValue { state in
            state.channels[identifier] = channel
            return state.phase == .accepting
        }
        channel.closeFuture.whenComplete { [self] _ in
            removeChannel(identifier)
        }
        if !accepted {
            channel.close(promise: nil)
        }
        return accepted
    }

    func beginSetup() -> ProxyConnectionSetupToken? {
        state.withLockedValue { state in
            guard state.phase == .accepting else { return nil }
            let identifier = UUID()
            let token = ProxyConnectionSetupToken(identifier: identifier) { [weak self] identifier in
                self?.removeSetup(identifier)
            }
            state.setupTokens[identifier] = token
            return token
        }
    }

    func beginShutdown() -> [Channel] {
        let resources = state.withLockedValue { state -> ShutdownResources in
            state.phase = .shuttingDown
            let cancellations = state.setupTokens.values.flatMap { $0.cancelFromRegistry() }
            return ShutdownResources(
                channels: Array(state.channels.values),
                cancellations: cancellations,
                waiters: drainWaitersIfQuiescent(&state)
            )
        }
        resources.cancellations.forEach { $0.schedule() }
        resources.waiters.forEach { $0.resume() }
        return resources.channels
    }

    func waitForQuiescence() async {
        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLockedValue { state in
                guard !state.isQuiescent else { return true }
                state.waiters.append(continuation)
                return false
            }
            if resumeImmediately {
                continuation.resume()
            }
        }
    }

    func finishShutdown() {
        state.withLockedValue { state in
            precondition(state.isQuiescent)
            precondition(state.waiters.isEmpty)
            state.phase = .idle
        }
    }

    private func removeChannel(_ identifier: ObjectIdentifier) {
        let waiters = state.withLockedValue { state -> [CheckedContinuation<Void, Never>] in
            state.channels.removeValue(forKey: identifier)
            return drainWaitersIfQuiescent(&state)
        }
        waiters.forEach { $0.resume() }
    }

    private func removeSetup(_ identifier: UUID) {
        let waiters = state.withLockedValue { state -> [CheckedContinuation<Void, Never>] in
            state.setupTokens.removeValue(forKey: identifier)
            return drainWaitersIfQuiescent(&state)
        }
        waiters.forEach { $0.resume() }
    }

    private func drainWaitersIfQuiescent(_ state: inout State) -> [CheckedContinuation<Void, Never>] {
        guard state.phase == .shuttingDown, state.isQuiescent else { return [] }
        defer { state.waiters.removeAll() }
        return state.waiters
    }
}

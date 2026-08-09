import NIOConcurrencyHelpers
import NIOCore

final class ProxyConnectionRegistry: Sendable {
    private enum Phase {
        case idle
        case accepting
        case shuttingDown
    }

    private struct State {
        var phase = Phase.idle
        var channels: [ObjectIdentifier: Channel] = [:]
        var inFlightConnections = 0
        var waiters: [CheckedContinuation<Void, Never>] = []

        var isQuiescent: Bool {
            channels.isEmpty && inFlightConnections == 0
        }
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
            remove(identifier)
        }
        if !accepted {
            channel.close(promise: nil)
        }
        return accepted
    }

    func beginConnection() -> Bool {
        state.withLockedValue { state in
            guard state.phase == .accepting else { return false }
            state.inFlightConnections += 1
            return true
        }
    }

    func completeConnection() {
        let waiters = state.withLockedValue { state -> [CheckedContinuation<Void, Never>] in
            precondition(state.inFlightConnections > 0)
            state.inFlightConnections -= 1
            return drainWaitersIfQuiescent(&state)
        }
        waiters.forEach { $0.resume() }
    }

    func beginShutdown() -> [Channel] {
        state.withLockedValue { state in
            state.phase = .shuttingDown
            return Array(state.channels.values)
        }
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

    private func remove(_ identifier: ObjectIdentifier) {
        let waiters = state.withLockedValue { state -> [CheckedContinuation<Void, Never>] in
            state.channels.removeValue(forKey: identifier)
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

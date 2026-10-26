import NIOConcurrencyHelpers

final class HTTPBreakpointCompletion: Sendable {
    private struct State {
        var completed = false
        var consumed = false
        var result: HTTPBreakpointResolution?
        var waiter: CheckedContinuation<HTTPBreakpointResolution, Never>?
    }

    private let state = NIOLockedValueBox(State())

    func wait() async -> HTTPBreakpointResolution {
        await withCheckedContinuation { continuation in
            let immediate = state.withLockedValue { state -> HTTPBreakpointResolution? in
                guard !state.consumed, state.waiter == nil else { return .cancelled }
                if state.completed {
                    state.consumed = true
                    let result = state.result
                    state.result = nil
                    return result
                }
                state.waiter = continuation
                return nil
            }
            if let immediate {
                continuation.resume(returning: immediate)
            }
        }
    }

    func complete(_ resolution: HTTPBreakpointResolution) {
        let waiter = state.withLockedValue { state in
            state.completed = true
            let waiter = state.waiter
            state.waiter = nil
            if waiter == nil {
                state.result = resolution
            }
            else {
                state.consumed = true
            }
            return waiter
        }
        waiter?.resume(returning: resolution)
    }
}

import Foundation
import os

enum DeliveryError: String, Error, Sendable {
    case overflow
    case cancelled
    case outputFailed
}

final class BoundedDelivery: Sendable {
    static let maximumEvents = 256
    static let maximumBytes = 1_048_576

    private struct State: Sendable {
        var slots = [Data?](repeating: nil, count: BoundedDelivery.maximumEvents)
        var head = 0
        var queuedEvents = 0
        var bytes = 0
        var finished = false
        var failure: DeliveryError?
        var waiter: CheckedContinuation<Data?, any Error>?

        mutating func terminate(_ error: DeliveryError) {
            failure = error
            slots = [Data?](repeating: nil, count: BoundedDelivery.maximumEvents)
            head = 0
            queuedEvents = 0
            bytes = 0
            waiter?.resume(throwing: error)
            waiter = nil
        }
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    var failure: DeliveryError? { state.withLock { $0.failure } }

    func offer(_ data: Data) {
        state.withLock { state in
            guard !state.finished, state.failure == nil else { return }
            guard data.count <= Self.maximumBytes - state.bytes,
                  state.queuedEvents < Self.maximumEvents else {
                state.terminate(.overflow)
                return
            }
            if let waiter = state.waiter {
                state.waiter = nil
                waiter.resume(returning: data)
            } else {
                state.slots[(state.head + state.queuedEvents) % Self.maximumEvents] = data
                state.queuedEvents += 1
                state.bytes += data.count
            }
        }
    }

    func fail(_ error: DeliveryError) {
        state.withLock { state in
            guard state.failure == nil else { return }
            state.terminate(error)
        }
    }

    func finish() {
        state.withLock { state in
            state.finished = true
            if state.queuedEvents == 0 {
                state.waiter?.resume(returning: nil)
                state.waiter = nil
            }
        }
    }

    func next() async throws -> Data? {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                state.withLock { state in
                    if let failure = state.failure {
                        continuation.resume(throwing: failure)
                    } else if state.queuedEvents > 0, let data = state.slots[state.head] {
                        state.slots[state.head] = nil
                        state.head = (state.head + 1) % Self.maximumEvents
                        state.queuedEvents -= 1
                        state.bytes -= data.count
                        continuation.resume(returning: data)
                    } else if state.finished {
                        continuation.resume(returning: nil)
                    } else {
                        state.waiter = continuation
                    }
                }
            }
        } onCancel: {
            state.withLock { state in
                if state.failure == nil {
                    state.terminate(.cancelled)
                }
            }
        }
    }
}

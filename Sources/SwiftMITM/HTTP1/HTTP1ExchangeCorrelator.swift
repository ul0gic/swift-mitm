import Foundation
import NIOConcurrencyHelpers

final class HTTP1ExchangeCorrelator: Sendable {
    struct Exchange: Sendable {
        let id: UUID
        let method: String
        let webSocketUpgradeRequested: Bool
    }

    private struct State {
        var queue: [Exchange] = []
        var acceptedWebSocketUpgradeID: UUID?
    }

    private let state = NIOLockedValueBox(State())

    func enqueue(id: UUID, method: String, webSocketUpgradeRequested: Bool = false) {
        state.withLockedValue {
            $0.queue.append(Exchange(
                id: id,
                method: method,
                webSocketUpgradeRequested: webSocketUpgradeRequested
            ))
        }
    }

    func peek() -> Exchange? {
        state.withLockedValue { $0.queue.first }
    }

    func dequeue() -> Exchange? {
        state.withLockedValue { $0.queue.isEmpty ? nil : $0.queue.removeFirst() }
    }

    func acceptWebSocketUpgrade(id: UUID) {
        state.withLockedValue { $0.acceptedWebSocketUpgradeID = id }
    }

    func takeAcceptedWebSocketUpgradeID() -> UUID? {
        state.withLockedValue { state in
            defer { state.acceptedWebSocketUpgradeID = nil }
            return state.acceptedWebSocketUpgradeID
        }
    }
}

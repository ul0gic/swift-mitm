import Foundation
import NIOConcurrencyHelpers

final class HTTP1ExchangeCorrelator: Sendable {
    struct Exchange: Sendable {
        let id: UUID
        let method: String
        let webSocketUpgradeRequested: Bool
        var requestEnded: Bool
        var responseEnded = false
        var published: Bool
        let admissionReservation: ResourceReservation?
    }

    private struct State {
        var queue: [Exchange?]
        var first = 0
        var responseIndex = 0
        var count = 0
        var locations: [UUID: Int] = [:]
        var methodBytes = 0
        var failed = false
        var acceptedWebSocketUpgradeID: UUID?
    }

    private let state: NIOLockedValueBox<State>
    private let policy: HTTP1CorrelationPolicy
    private let admission: ResourceAdmissionController?

    init(policy: HTTP1CorrelationPolicy = .default, admission: ResourceAdmissionController? = nil) {
        self.policy = policy
        self.admission = admission
        state = NIOLockedValueBox(State(queue: Array(repeating: nil, count: policy.maximumPendingExchanges)))
    }

    @discardableResult
    func enqueue(
        id: UUID,
        method: String,
        webSocketUpgradeRequested: Bool = false,
        requestEnded: Bool = true,
        published: Bool = true
    ) -> Bool {
        state.withLockedValue { state in
            let bytes = method.utf8.count
            guard !state.failed, state.count < policy.maximumPendingExchanges,
                bytes <= policy.maximumPendingMethodBytes - state.methodBytes else { return false }
            let reservation: ResourceReservation?
            do { reservation = try admission?.acquire(.stream) }
            catch { return false }
            let index = (state.first + state.count) % state.queue.count
            state.queue[index] = Exchange(
                id: id,
                method: method,
                webSocketUpgradeRequested: webSocketUpgradeRequested,
                requestEnded: requestEnded,
                published: published,
                admissionReservation: reservation
            )
            state.locations[id] = index
            state.count += 1
            state.methodBytes += bytes
            return true
        }
    }

    func peek() -> Exchange? {
        state.withLockedValue { state in
            guard let exchange = state.queue[state.responseIndex], !exchange.responseEnded else { return nil }
            return exchange
        }
    }

    var isFailed: Bool {
        state.withLockedValue { $0.failed }
    }

    func publish(id: UUID) {
        state.withLockedValue { state in
            guard let index = state.locations[id] else { return }
            state.queue[index]?.published = true
        }
    }

    func end(id: UUID, request: Bool) {
        state.withLockedValue { state in
            guard let index = state.locations[id] else { return }
            if request {
                state.queue[index]?.requestEnded = true
            } else {
                state.queue[index]?.responseEnded = true
                state.responseIndex = (index + 1) % state.queue.count
            }
            while let exchange = state.queue[state.first], exchange.requestEnded, exchange.responseEnded {
                exchange.admissionReservation?.release()
                state.queue[state.first] = nil
                state.locations.removeValue(forKey: exchange.id)
                state.first = (state.first + 1) % state.queue.count
                state.count -= 1
                state.methodBytes -= exchange.method.utf8.count
            }
        }
    }

    func failOutstanding(sink: CaptureEventSink, message: String) {
        let exchanges = state.withLockedValue { state in
            guard !state.failed else { return [Exchange]() }
            state.failed = true
            let exchanges = (0..<state.count).compactMap { state.queue[(state.first + $0) % state.queue.count] }
            state.queue = Array(repeating: nil, count: state.queue.count)
            state.locations.removeAll(keepingCapacity: false)
            state.count = 0
            state.methodBytes = 0
            state.acceptedWebSocketUpgradeID = nil
            return exchanges
        }
        for exchange in exchanges {
            exchange.admissionReservation?.release()
            if exchange.published {
                sink.receive(.streamError(requestID: exchange.id, message: message))
            }
        }
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

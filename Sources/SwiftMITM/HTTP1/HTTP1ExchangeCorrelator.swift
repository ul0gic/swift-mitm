import Foundation
import NIOConcurrencyHelpers

final class HTTP1ExchangeCorrelator: Sendable {
    struct Exchange: Sendable {
        let id: UUID
        let method: String
    }

    private let queue = NIOLockedValueBox<[Exchange]>([])

    func enqueue(id: UUID, method: String) {
        queue.withLockedValue { $0.append(Exchange(id: id, method: method)) }
    }

    func peek() -> Exchange? {
        queue.withLockedValue { $0.first }
    }

    func dequeue() -> Exchange? {
        queue.withLockedValue { $0.isEmpty ? nil : $0.removeFirst() }
    }
}

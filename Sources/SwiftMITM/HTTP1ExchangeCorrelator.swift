import Foundation
import NIOConcurrencyHelpers

/// Correlates request/response pairs across the two HTTP/1.1 legs of one keep-alive connection.
/// Responses arrive in request order, so a FIFO suffices; `method` lets the response leg frame HEAD bodies.
final class HTTP1ExchangeCorrelator: Sendable {
    struct Exchange: Sendable {
        let id: UUID
        let method: String
    }

    private let queue = NIOLockedValueBox<[Exchange]>([])

    func enqueue(id: UUID, method: String) {
        queue.withLockedValue { $0.append(Exchange(id: id, method: method)) }
    }

    func dequeue() -> Exchange? {
        queue.withLockedValue { $0.isEmpty ? nil : $0.removeFirst() }
    }
}

import NIOConcurrencyHelpers
import NIOCore

final class Phase2FixtureCompletion<Value: Sendable>: @unchecked Sendable {
    let futureResult: EventLoopFuture<Value>

    private let lock = NIOLock()
    private let promise: EventLoopPromise<Value>
    private var completed = false
    private var deadline: Scheduled<Void>?

    init(eventLoop: EventLoop, timeout: TimeAmount = .seconds(2)) {
        promise = eventLoop.makePromise()
        futureResult = promise.futureResult
        deadline = eventLoop.scheduleTask(in: timeout) { [weak self] in
            self?.complete(.failure(Phase2FixtureError.deadlineExceeded))
        }
    }

    func complete(_ result: Result<Value, Error>) {
        let deadline = lock.withLock { () -> Scheduled<Void>? in
            guard !completed else { return nil }
            completed = true
            return self.deadline
        }
        guard let deadline else { return }
        deadline.cancel()
        promise.completeWith(result)
    }
}

import NIOConcurrencyHelpers
import NIOCore

final class ProxySetupStageCompletion<Value: Sendable>: Sendable {
    let futureResult: EventLoopFuture<Value>

    private let promise: EventLoopPromise<Value>
    private let completed = NIOLockedValueBox(false)

    init(eventLoop: EventLoop) {
        promise = eventLoop.makePromise()
        futureResult = promise.futureResult
    }

    @discardableResult
    func complete(_ result: Result<Value, Error>) -> Bool {
        let claimed = completed.withLockedValue { completed in
            guard !completed else { return false }
            completed = true
            return true
        }
        if claimed {
            promise.completeWith(result)
        }
        return claimed
    }
}

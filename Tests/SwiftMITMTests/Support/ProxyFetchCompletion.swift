import NIOConcurrencyHelpers
import NIOCore

enum ProxyFetchPhase: String, Sendable {
    case connecting
    case tlsHandshake
    case request
    case response
}

enum ProxyTestError: Error {
    case connectionClosedBeforeResponseEnd
    case connectFailed(String)
    case fetchTimeout(phase: String, bytesReceived: Int)
    case stalledFetchTimeout(bytesReceived: Int)
    case tlsClosedBeforeHandshake
    case unexpectedALPN
}

final class ProxyFetchCompletion: @unchecked Sendable {
    let futureResult: EventLoopFuture<Int>

    private let lock = NIOLock()
    private let promise: EventLoopPromise<Int>
    private var completed = false
    private var phase = ProxyFetchPhase.connecting
    private var receivedBytes = 0

    init(eventLoop: EventLoop) {
        promise = eventLoop.makePromise()
        futureResult = promise.futureResult
    }

    func advance(to phase: ProxyFetchPhase) {
        lock.withLock { self.phase = phase }
    }

    func addReceivedBytes(_ count: Int) {
        lock.withLock {
            receivedBytes += count
            phase = .response
        }
    }

    func complete(_ result: Result<Int, Error>) {
        let shouldComplete = lock.withLock {
            guard !completed else { return false }
            completed = true
            return true
        }
        if shouldComplete {
            promise.completeWith(result)
        }
    }

    func timeoutError() -> ProxyTestError {
        lock.withLock { .fetchTimeout(phase: phase.rawValue, bytesReceived: receivedBytes) }
    }
}

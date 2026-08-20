import NIOConcurrencyHelpers
import NIOCore

func phase4BoundedWait<Value: Sendable>(
    _ future: EventLoopFuture<Value>,
    timeout: TimeAmount = .seconds(2)
) throws -> Value {
    let completion = Phase2FixtureCompletion<Value>(eventLoop: future.eventLoop, timeout: timeout)
    future.whenComplete { completion.complete($0) }
    return try completion.futureResult.wait()
}

final class Phase4ExactBytes: @unchecked Sendable {
    let futureResult: EventLoopFuture<[UInt8]>

    private let lock = NIOLock()
    private let completion: Phase2FixtureCompletion<[UInt8]>
    private let expected: [UInt8]
    private let maximumBytes: Int
    private var bytes: [UInt8] = []
    private var completed = false

    init(
        eventLoop: EventLoop,
        expected: [UInt8],
        maximumBytes: Int,
        timeout: TimeAmount = .seconds(2)
    ) {
        self.expected = expected
        self.maximumBytes = maximumBytes
        completion = Phase2FixtureCompletion(eventLoop: eventLoop, timeout: timeout)
        futureResult = completion.futureResult
        if expected.isEmpty {
            completed = true
            completion.complete(.success([]))
        }
    }

    var observedBytes: [UInt8] { lock.withLock { bytes } }

    func append(_ incoming: ByteBufferView) {
        completeIfReady(Array(incoming))
    }

    func append(_ incoming: [UInt8]) {
        completeIfReady(incoming)
    }

    func close() {
        let shouldFail = lock.withLock {
            guard !completed else { return false }
            completed = true
            return true
        }
        if shouldFail {
            completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
    }

    private func completeIfReady(_ incoming: [UInt8]) {
        let result = lock.withLock { () -> Result<[UInt8], Error>? in
            guard !completed else { return nil }
            bytes += incoming
            guard bytes.count <= maximumBytes else {
                completed = true
                return .failure(Phase2FixtureError.exceededByteLimit)
            }
            guard bytes == Array(expected.prefix(bytes.count)) else {
                completed = true
                return .failure(Phase2FixtureError.unexpectedBytes)
            }
            guard bytes.count >= expected.count else { return nil }
            completed = true
            return .success(bytes)
        }
        if let result {
            completion.complete(result)
        }
    }
}

final class Phase4TerminalObservation: @unchecked Sendable {
    struct Snapshot: Equatable, Sendable {
        var inputClosedCount = 0
        var inactiveCount = 0
        var errorCount = 0
    }

    private let storage = NIOLockedValueBox(Snapshot())

    var snapshot: Snapshot { storage.withLockedValue { $0 } }

    func recordInputClosed() {
        storage.withLockedValue { $0.inputClosedCount += 1 }
    }

    func recordInactive() {
        storage.withLockedValue { $0.inactiveCount += 1 }
    }

    func recordError() {
        storage.withLockedValue { $0.errorCount += 1 }
    }
}

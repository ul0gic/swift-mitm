import NIOCore

func phase4BoundedWait<Value: Sendable>(
    _ future: EventLoopFuture<Value>,
    timeout: TimeAmount = .seconds(2)
) throws -> Value {
    let completion = Phase2FixtureCompletion<Value>(eventLoop: future.eventLoop, timeout: timeout)
    future.whenComplete { completion.complete($0) }
    return try completion.futureResult.wait()
}

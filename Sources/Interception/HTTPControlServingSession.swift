import Foundation

/// End the session explicitly; dropping this handle does not stop its runtime.
public final class HTTPControlServingSession: Sendable {
    private let owner: UUID
    private let runtime: HTTPControlRuntime

    init(owner: UUID, runtime: HTTPControlRuntime) {
        self.owner = owner
        self.runtime = runtime
    }

    public func end() async -> HTTPControlStopReport? {
        await runtime.endServing(owner: owner)
    }
}

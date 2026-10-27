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

public extension HTTPControlRuntime {
    func startServing(
        admission: ResourceAdmissionController? = nil,
        transportAdmission: ResourceAdmissionController? = nil
    ) throws -> HTTPControlServingSession {
        let owner = UUID()
        try beginServing(owner: owner, admission: admission, transportAdmission: transportAdmission)
        return HTTPControlServingSession(owner: owner, runtime: self)
    }
}

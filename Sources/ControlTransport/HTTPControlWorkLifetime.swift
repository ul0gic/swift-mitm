public struct HTTPControlWorkLifetime: Sendable {
    let controls: [HTTPControlByteReservation]
    let global: [ResourceReservation]

    init(controls: [HTTPControlByteReservation], global: [ResourceReservation]) {
        self.controls = controls
        self.global = global
    }

    public init(
        decision: HTTPControlDecision,
        runtime: HTTPControlRuntime,
        admission: ResourceAdmissionController?
    ) throws {
        var bytes = decision.retainedBytes
        if case let .replaceBody(_, maximum, _, _) = decision.action {
            let (total, overflow) = bytes.addingReportingOverflow(maximum)
            guard maximum > 0, !overflow else { throw HTTPControlError.invalidConfiguration }
            bytes = total
        }
        try self.init(bytes: bytes, runtime: runtime, admission: admission)
    }

    public init(
        configuration: HTTPControlConfiguration,
        runtime: HTTPControlRuntime,
        admission: ResourceAdmissionController?
    ) throws {
        try self.init(
            bytes: configuration.retainedBytes,
            runtime: runtime,
            admission: admission
        )
    }

    private init(
        bytes: Int,
        runtime: HTTPControlRuntime,
        admission: ResourceAdmissionController?
    ) throws {
        guard bytes > 0 else {
            controls = []
            global = []
            return
        }
        let control = try runtime.reserveTransportBytes(bytes)
        do {
            global = try Self.reserveAdmission(bytes, admission: admission)
            controls = [control]
        } catch {
            control.release()
            throw error
        }
    }

    static func reserveAdmission(
        _ bytes: Int,
        admission: ResourceAdmissionController?
    ) throws -> [ResourceReservation] {
        guard let admission else { return [] }
        return [try admission.acquire(.retainedBytes(bytes))]
    }
}

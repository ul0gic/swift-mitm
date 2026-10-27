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
        admission: ResourceAdmissionController?,
        transportAdmission: ResourceAdmissionController? = nil
    ) throws {
        var bytes = decision.retainedBytes
        if case let .replaceBody(_, maximum, _, _) = decision.action {
            let (total, overflow) = bytes.addingReportingOverflow(maximum)
            guard maximum > 0, !overflow else { throw HTTPControlError.invalidConfiguration }
            bytes = total
        }
        try self.init(bytes: bytes, runtime: runtime, admission: admission, transportAdmission: transportAdmission)
    }

    public init(
        configuration: HTTPControlConfiguration,
        runtime: HTTPControlRuntime,
        admission: ResourceAdmissionController?,
        transportAdmission: ResourceAdmissionController? = nil
    ) throws {
        try self.init(
            bytes: configuration.retainedBytes,
            runtime: runtime,
            admission: admission,
            transportAdmission: transportAdmission
        )
    }

    private init(
        bytes: Int,
        runtime: HTTPControlRuntime,
        admission: ResourceAdmissionController?,
        transportAdmission: ResourceAdmissionController?
    ) throws {
        guard bytes > 0 else {
            controls = []
            global = []
            return
        }
        let control = try runtime.reserveTransportBytes(bytes)
        do {
            global = try Self.reserveAdmission(bytes, admission: admission, transportAdmission: transportAdmission)
            controls = [control]
        } catch {
            control.release()
            throw error
        }
    }

    static func reserveAdmission(
        _ bytes: Int,
        admission: ResourceAdmissionController?,
        transportAdmission: ResourceAdmissionController?
    ) throws -> [ResourceReservation] {
        var reservations: [ResourceReservation] = []
        do {
            if let admission {
                reservations.append(try admission.acquire(.retainedBytes(bytes)))
            }
            if let transportAdmission, transportAdmission !== admission {
                reservations.append(try transportAdmission.acquire(.retainedBytes(bytes)))
            }
            return reservations
        } catch {
            for reservation in reservations { reservation.release() }
            throw error
        }
    }
}

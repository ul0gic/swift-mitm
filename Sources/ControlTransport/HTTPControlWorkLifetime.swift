struct HTTPControlWorkLifetime: Sendable {
    let controls: [HTTPControlByteReservation]
    let global: [ResourceReservation]

    init(controls: [HTTPControlByteReservation], global: [ResourceReservation]) {
        self.controls = controls
        self.global = global
    }

    init(
        decision: HTTPControlDecision,
        runtime: HTTPControlRuntime,
        admission: ResourceAdmissionController?
    ) throws {
        var bytes = decision.retainedBytes
        if case .replaceBody(_, let maximum, _, _) = decision.action { bytes += maximum }
        if bytes > 0 {
            controls = [try runtime.reserveTransportBytes(bytes)]
            if let admission { global = [try admission.acquire(.retainedBytes(bytes))] }
            else { global = [] }
        } else {
            controls = []
            global = []
        }
    }
}

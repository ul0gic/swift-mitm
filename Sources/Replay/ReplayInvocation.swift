import Foundation

struct ReplayIdentity: Sendable {
    let requestID: UUID
    let attemptID: UUID
    let source: ReplaySource

    func result(
        status: Int, responseBodyBytes: Int, headers: [HTTPHeaderField], measurement: ReplayMeasurement
    ) -> ReplayResult {
        ReplayResult(
            attemptID: attemptID, requestID: requestID,
            sourceRequestID: source.requestID, sourceRepresentation: source.representation,
            status: status, responseBodyBytes: responseBodyBytes,
            timings: measurement.timings, timingUnavailable: measurement.unavailable,
            responseContentBytes: Self.identityCoding(headers) ? responseBodyBytes : nil
        )
    }

    private static func identityCoding(_ headers: [HTTPHeaderField]) -> Bool {
        for header in headers {
            let name = header.name.lowercased()
            guard name == "content-encoding" || name == "transfer-encoding" else { continue }
            let allowed: Set<String> = name == "content-encoding" ? ["identity"] : ["identity", "chunked"]
            let codings = header.value.lowercased().split(separator: ",", omittingEmptySubsequences: false)
            guard codings.allSatisfy({ allowed.contains($0.trimmingCharacters(in: .whitespaces)) }) else {
                return false
            }
        }
        return true
    }
}

struct ReplayInvocation: Sendable {
    let source: ReplayRequest
    let effective: ReplayRequest
    let original: ReplayPreparedRequest
    let prepared: ReplayPreparedRequest
    let identity: ReplayIdentity
    let controls: ReplayControls?
    let faults: [HTTPFault]
    let responseSink: any ReplayResponseSink

    func respond(
        status: Int, headers: [HTTPHeaderField], body: Data, server: ProxyServer
    ) async throws -> ReplayResult {
        let response = CapturedResponseHead(
            requestID: identity.requestID, timestamp: Date(), status: status,
            version: .http11, headers: headers
        )
        var processor = ReplayResponseControls(
            controls: controls, inheritedFaults: faults, attemptID: identity.attemptID,
            responseSink: responseSink, observation: server.sink as? ObservationCaptureSink
        )
        try await processor.head(response)
        var offset = 0
        while offset < body.count {
            let end = min(offset + 65_536, body.count)
            try await processor.body(Array(body[offset..<end]), requestID: identity.requestID)
            offset = end
        }
        try await processor.end()
        let result = identity.result(
            status: processor.effectiveStatus ?? status, responseBodyBytes: processor.effectiveBodyBytes,
            headers: processor.effectiveHeaders, measurement: .synthetic
        )
        try await responseSink.receive(.end(result))
        return result
    }
}

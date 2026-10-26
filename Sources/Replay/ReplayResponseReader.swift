import Foundation
import NIOCore
import NIOHTTP1

struct ReplayResponseReader: Sendable {
    let execution: ReplayExecution
    let timing: ReplayTimingRecorder
    private var capture: CaptureBodyBuffer
    private var status: Int?
    private var receivedBytes = 0
    private var controls: ReplayResponseControls

    init(execution: ReplayExecution, timing: ReplayTimingRecorder) {
        self.execution = execution
        self.timing = timing
        capture = CaptureBodyBuffer(limit: execution.server.captureBodyLimit)
        let invocation = execution.invocation
        controls = ReplayResponseControls(
            controls: invocation.controls,
            inheritedFaults: invocation.faults,
            attemptID: invocation.identity.attemptID,
            responseSink: invocation.responseSink,
            observation: execution.server.sink as? ObservationCaptureSink
        )
    }

    mutating func receive(_ part: HTTPClientResponsePart) async throws -> ReplayResult? {
        switch part {
        case let .head(head): return try await receiveHead(head)
        case let .body(body):
            try await receiveBody(body)
            return nil
        case let .end(trailers): return try await receiveEnd(trailers)
        }
    }

    private mutating func receiveHead(_ received: HTTPResponseHead) async throws -> ReplayResult? {
        timing.startResponse()
        guard !ExplicitHTTPProxyHandler.ambiguous(received.headers), received.status.code != 101,
            status == nil else { throw ReplayError.incompleteResponse }
        let response = CapturedResponseHead(
            requestID: execution.invocation.identity.requestID,
            timestamp: Date(),
            status: Int(received.status.code),
            version: .http11,
            headers: ExplicitHTTPProxyHandler.fields(received.headers)
        )
        if received.status.code >= 200 {
            status = Int(received.status.code)
        }
        execution.server.sink.receive(.responseHead(response))
        try await controls.head(response)
        guard controls.completesAtHead else { return nil }
        timing.finishResponse()
        try await controls.end()
        execution.server.sink.receive(.responseEnd(requestID: response.requestID, truncated: true))
        return result(status: response.status, measurement: .synthetic)
    }

    private mutating func receiveBody(_ body: ByteBuffer) async throws {
        guard status != nil else { throw ReplayError.incompleteResponse }
        let (newCount, overflow) = receivedBytes.addingReportingOverflow(body.readableBytes)
        guard !overflow else { throw ReplayError.incompleteResponse }
        receivedBytes = newCount
        let requestID = execution.invocation
.identity
.requestID
        (execution.server.sink as? ObservationCaptureSink)?.receiveBody(
            requestID: requestID,
            direction: .response,
            bytes: body.readableBytesView
        )
        execution.server.sink.receive(.responseBodyChunk(
            requestID: requestID,
            bytes: capture.take(body.readableBytesView),
            byteCount: body.readableBytes
        ))
        var buffer = body
        while let bytes = buffer.readBytes(length: min(65_536, buffer.readableBytes)), !bytes.isEmpty {
            try await controls.body(bytes, requestID: requestID)
            try Task.checkCancellation()
        }
    }

    private mutating func receiveEnd(_ trailers: HTTPHeaders?) async throws -> ReplayResult? {
        guard let status else { return nil }
        timing.finishResponse()
        try await controls.end()
        let invocation = execution.invocation
        if let trailers, !trailers.isEmpty {
            let fields = ExplicitHTTPProxyHandler.fields(trailers)
            execution.server.sink.receive(.responseTrailers(requestID: invocation.identity.requestID, headers: fields))
            if !controls.representationChanged {
                try await invocation.responseSink.receive(.trailers(
                    attemptID: invocation.identity.attemptID,
                    requestID: invocation.identity.requestID,
                    headers: fields
                ))
            }
        }
        execution.server.sink.receive(.responseEnd(
            requestID: invocation.identity.requestID,
            truncated: capture.truncated
        ))
        return result(status: status, measurement: try timing.measurement())
    }

    private func result(status: Int, measurement: ReplayMeasurement) -> ReplayResult {
        execution.invocation.identity.result(
            status: controls.effectiveStatus ?? status,
            responseBodyBytes: controls.effectiveBodyBytes,
            headers: controls.effectiveHeaders,
            measurement: measurement
        )
    }
}

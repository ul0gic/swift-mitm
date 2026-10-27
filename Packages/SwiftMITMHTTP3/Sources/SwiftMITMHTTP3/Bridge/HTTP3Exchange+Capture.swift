import Foundation
import HTTPTypes
import NIOCore
import SwiftMITM

extension HTTP3Exchange {
    func emit(_ event: CaptureEvent, representation: HTTPRepresentationKind) {
        guard representation != .original || !originalFailed else { return }
        if representation == .original { transport.sink.receive(event) }
        transport.observationSink?.receive(event, representation: representation)
    }

    func terminateOriginal(_ message: String) {
        guard !originalFailed else { return }
        originalFailed = true
        let event = CaptureEvent.streamError(requestID: id, message: message)
        transport.sink.receive(event)
        transport.observationSink?.receive(event, representation: .original)
    }

    func captureHead(
        _ message: HTTP3Message,
        fields: [HTTPHeaderField],
        representation: HTTPRepresentationKind
    ) {
        switch message {
        case let .request(.head(head)):
            emit(.requestHead(CapturedRequestHead(
                id: id, timestamp: Date(), scheme: head.scheme ?? "https",
                authority: head.authority ?? transport.configuration.route.authority,
                method: head.method.rawValue, path: head.path ?? "/", version: .http3, headers: fields,
                target: representation == .original ? downstream.target : (origin?.target ?? downstream.target)
            )), representation: representation)
        case let .response(.head(head)):
            emit(.responseHead(CapturedResponseHead(
                requestID: id, timestamp: Date(), status: head.status.code, version: .http3, headers: fields
            )), representation: representation)
        default: break
        }
    }

    func captureBody(_ body: ByteBuffer, flow: HTTP3Flow, representation: HTTPRepresentationKind) throws {
        guard representation != .original || !originalFailed else { return }
        let observation = HTTPBodyObservation(requestID: id, direction: flow.observation, representation: representation)
        body.withUnsafeReadableBytes { transport.observationSink?.receiveBody(observation, bytes: $0) }
        let used = representation == .original ? flow.capturedBytes : flow.effectiveBytes
        let count = min(body.readableBytes, max(0, transport.configuration.captureBodyLimit - used))
        let lease = count > 0 ? try transport.resources.reserve(count, kind: .capture) : nil
        let bytes = Array(body.readableBytesView.prefix(count))
        let event: CaptureEvent = flow.isRequest
            ? .requestBodyChunk(requestID: id, bytes: bytes, byteCount: body.readableBytes)
            : .responseBodyChunk(requestID: id, bytes: bytes, byteCount: body.readableBytes)
        withExtendedLifetime(lease) { emit(event, representation: representation) }
        if representation == .original {
            flow.capturedBytes += count
            flow.captureTruncated = flow.captureTruncated || count < body.readableBytes
        } else {
            flow.effectiveBytes += count
            flow.effectiveTruncated = flow.effectiveTruncated || count < body.readableBytes
        }
    }

    func captureEnd(_ flow: HTTP3Flow, representation: HTTPRepresentationKind, truncated: Bool = false) {
        let limited = representation == .original ? flow.captureTruncated : flow.effectiveTruncated
        let event: CaptureEvent = flow.isRequest
            ? .requestEnd(requestID: id, truncated: truncated || limited)
            : .responseEnd(requestID: id, truncated: truncated || limited)
        emit(event, representation: representation)
    }

    func captureTrailers(_ fields: [HTTPHeaderField], flow: HTTP3Flow, representation: HTTPRepresentationKind) {
        guard !fields.isEmpty else { return }
        let event: CaptureEvent = flow.isRequest
            ? .requestTrailers(requestID: id, headers: fields)
            : .responseTrailers(requestID: id, headers: fields)
        emit(event, representation: representation)
    }
}

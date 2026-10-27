import Foundation
import NIOConcurrencyHelpers
import SwiftMITM

struct HostField: Encodable, Sendable {
    let name: String
    let value: String
    let rawValueBytes: [UInt8]?

    init(_ field: HTTPHeaderField) {
        name = field.name
        value = field.value
        rawValueBytes = field.rawValueBytes
    }
}

struct HostEvent: Encodable, Sendable {
    let kind: String
    let requestID: UUID?
    var path: String?
    var version: String?
    var authority: String?
    var destinationAddress: String?
    var destinationPort: Int?
    var provenance: String?
    var headers: [HostField]?
    var byteCount: Int?
    var truncated: Bool?
    var status: Int?
    var failure: String?
}

final class HostCapture: CaptureEventSink {
    private struct State: Sendable {
        var events: [HostEvent] = []
        var bytes = 0
        var overflow = false
    }

    private let state = NIOLockedValueBox(State())

    func receive(_ event: CaptureEvent) {
        let projected = project(event)
        let retained = projected.headers?
            .reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count + ($1.rawValueBytes?.count ?? 0) } ?? 0
        state.withLockedValue {
            guard $0.events.count < 20_000, $0.bytes + retained <= 8 * 1024 * 1024 else {
                $0.overflow = true
                return
            }
            $0.bytes += retained
            $0.events.append(projected)
        }
    }

    func write(to path: String) throws {
        let events = try state.withLockedValue {
            guard !$0.overflow else { throw HostError.captureOverflow }
            return $0.events
        }
        try JSONEncoder().encode(events).write(to: URL(fileURLWithPath: path), options: .atomic)
    }

    private func project(_ event: CaptureEvent) -> HostEvent {
        switch event {
        case let .requestHead(head):
            var result = HostEvent(kind: "requestHead", requestID: head.id)
            result.path = head.path
            result.version = head.version.rawValue
            result.authority = head.authority
            result.destinationAddress = head.target?.destination.address
            result.destinationPort = head.target?.destination.port
            result.provenance = head.target?.ingressProvenance.rawValue
            result.headers = head.headers.map(HostField.init)
            return result
        case let .responseHead(head):
            var result = HostEvent(kind: "responseHead", requestID: head.requestID)
            result.version = head.version.rawValue
            result.status = head.status
            result.headers = head.headers.map(HostField.init)
            return result
        case let .requestTrailers(id, headers), let .responseTrailers(id, headers):
            let kind: String
            if case .requestTrailers = event {
                kind = "requestTrailers"
            } else {
                kind = "responseTrailers"
            }
            var result = HostEvent(kind: kind, requestID: id)
            result.headers = headers.map(HostField.init)
            return result
        default:
            return projectStream(event)
        }
    }

    private func projectStream(_ event: CaptureEvent) -> HostEvent {
        switch event {
        case let .requestEnd(id, truncated), let .responseEnd(id, truncated):
            let kind: String
            if case .requestEnd = event {
                kind = "requestEnd"
            } else {
                kind = "responseEnd"
            }
            var result = HostEvent(kind: kind, requestID: id)
            result.truncated = truncated
            return result
        case let .requestBodyChunk(id, _, count), let .responseBodyChunk(id, _, count):
            let kind: String
            if case .requestBodyChunk = event {
                kind = "requestBodyChunk"
            } else {
                kind = "responseBodyChunk"
            }
            var result = HostEvent(kind: kind, requestID: id)
            result.byteCount = count
            return result
        case let .streamError(id, message):
            var result = HostEvent(kind: "streamError", requestID: id)
            result.failure = String(message.prefix(512))
            return result
        case let .connectionFailure(failure):
            var result = HostEvent(kind: "connectionFailure", requestID: failure.id)
            result.failure = "\(failure.reason):\(String(describing: failure.code))"
            return result
        default:
            return HostEvent(kind: "unexpectedNonHTTPEvent", requestID: nil)
        }
    }
}

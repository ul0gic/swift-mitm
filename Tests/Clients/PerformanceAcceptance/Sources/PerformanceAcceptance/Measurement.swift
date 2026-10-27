import Dispatch
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import SwiftMITM

enum MeasurementError: Error {
    case arguments
    case protocolMismatch
    case responseMismatch
    case unexpectedClosure
    case timeout
    case captureMismatch
    case invalidClockInterval
}

final class Completion<Value: Sendable>: Sendable {
    let promise: EventLoopPromise<Value>
    private let completed = NIOLockedValueBox(false)

    init(on loop: EventLoop) {
        promise = loop.makePromise(of: Value.self)
    }

    func complete(_ result: Result<Value, Error>) {
        let first = completed.withLockedValue { value in
            guard !value else { return false }
            value = true
            return true
        }
        if first {
            promise.completeWith(result)
        }
    }

    func wait(seconds: Int64) throws -> Value {
        let timeout = promise.futureResult.eventLoop.scheduleTask(in: .seconds(seconds)) {
            self.complete(.failure(MeasurementError.timeout))
        }
        defer { timeout.cancel() }
        return try promise.futureResult.wait()
    }
}

struct CaptureRecord: Codable, Sendable {
    let requestID: UUID
    let path: String
    let protocolName: String
    let headTime: UInt64
    var requestEnds = 0
    var responseHeads = 0
    var responseEnds = 0
    var responseBytes = 0
    var status: Int?
    var responseBodyEvents: Int?
}

final class MeasurementSink: CaptureEventSink {
    private struct State {
        var records: [UUID: CaptureRecord] = [:]
        var failures = 0
    }

    private let state = NIOLockedValueBox(State())
    private let diagnosticCallbacks: Bool

    init(diagnosticCallbacks: Bool = false) {
        self.diagnosticCallbacks = diagnosticCallbacks
    }

    func receive(_ event: CaptureEvent) {
        let timestamp = DispatchTime.now().uptimeNanoseconds
        state.withLockedValue { state in
            switch event {
            case let .requestHead(head):
                guard state.records.count < 102, state.records[head.id] == nil else {
                    state.failures += 1
                    return
                }
                state.records[head.id] = CaptureRecord(
                    requestID: head.id,
                    path: head.path,
                    protocolName: head.version.rawValue,
                    headTime: timestamp,
                    responseBodyEvents: diagnosticCallbacks ? 0 : nil
                )
            case let .requestEnd(id, _): state.records[id]?.requestEnds += 1
            case let .responseHead(head):
                state.records[head.requestID]?.responseHeads += 1
                state.records[head.requestID]?.status = head.status
            case let .responseBodyChunk(id, _, count):
                state.records[id]?.responseBytes += count
                if let events = state.records[id]?.responseBodyEvents {
                    state.records[id]?.responseBodyEvents = events + 1
                }
            case let .responseEnd(id, _): state.records[id]?.responseEnds += 1
            case .streamError, .connectionFailure: state.failures += 1
            default: break
            }
        }
    }

    func snapshot() throws -> [CaptureRecord] {
        try state.withLockedValue { state in
            guard state.failures == 0 else { throw MeasurementError.captureMismatch }
            return Array(state.records.values)
        }
    }
}

struct ExchangeMeasurement: Codable, Sendable {
    let path: String
    let start: UInt64
    let end: UInt64
    let bytes: Int
    let sha256: String
    let bodyCallbacks: BodyCallbackObservation?
}

struct BodyCallbackObservation: Codable, Sendable {
    var count = 0
    var zeroByteCount = 0
    var minimumNonemptyBytes: Int?
    var maximumNonemptyBytes: Int?

    mutating func record(bytes: Int) {
        count += 1
        if bytes == 0 {
            zeroByteCount += 1
        } else {
            minimumNonemptyBytes = min(minimumNonemptyBytes ?? bytes, bytes)
            maximumNonemptyBytes = max(maximumNonemptyBytes ?? bytes, bytes)
        }
    }
}

struct BenchmarkResult: Codable, Sendable {
    let protocolName: String
    let workload: String
    let downstreamALPN: String
    let upstreamALPN: [String]
    let originPaths: [String]
    let exchanges: [ExchangeMeasurement]
    let captures: [CaptureRecord]
    let processObservations: ProcessObservations?
}

import Darwin
import Foundation
import os
import SwiftMITM

enum Console {
    static func error(_ message: String) {
        do {
            try FileHandle.standardError.write(contentsOf: Data((message + "\n").utf8))
        } catch {
            exit(1)
        }
    }

    static func line(_ data: Data) {
        do {
            try FileHandle.standardOutput.write(contentsOf: data + Data([10]))
        } catch {
            Self.error("FirstCapture stdout delivery failed.")
            exit(1)
        }
    }
}

struct OutputRecord: Encodable, Sendable {
    var type: String
    var id: UUID?
    var port: Int?
    var method: String?
    var authority: String?
    var status: Int?
    var byteCount: Int?
    var retainedBytes: Int?
    var payloadBase64: String?
    var truncated: Bool?
    var reason: String?
    var stage: String?
    var code: String?
    var direction: String?
    var allActiveCapturesIncomplete: Bool?

    init(type: String) {
        self.type = type
    }

    init(_ event: CaptureEvent) {
        let summary = event.outputSummary
        self.init(type: summary.0)
        id = summary.1
        switch event {
        case .requestHead(let head):
            method = String(head.method.prefix(256))
            authority = String(head.authority.prefix(256))
        case .responseHead(let head):
            status = head.status
        case .requestBodyChunk(_, let bytes, let count), .responseBodyChunk(_, let bytes, let count):
            payload(bytes, count: count)
        case .requestEnd(_, let isTruncated), .responseEnd(_, let isTruncated):
            truncated = isTruncated
        case .webSocketFrame(let frame):
            direction = frame.direction.rawValue
            payload(frame.bytes, count: frame.byteCount)
            truncated = frame.truncated
        case .opaqueOpen(let flow):
            authority = String(flow.target.logicalAuthority.prefix(256))
            reason = flow.reason.rawValue
        case .opaqueData(_, _, let flowDirection, let bytes, let count):
            direction = flowDirection.rawValue
            payload(bytes, count: count)
        case .opaqueDirectionEnd(_, _, let flowDirection, let count, let isTruncated):
            direction = flowDirection.rawValue
            byteCount = count
            truncated = isTruncated
        case .opaqueClose(_, _, let closeReason):
            reason = closeReason.rawValue
        case .opaqueError(_, _, let failureReason):
            reason = failureReason.rawValue
        case .connectionFailure(let failure):
            reason = failure.reason.rawValue
            stage = failure.stage?.rawValue
            code = failure.code?.rawValue
        case .requestTrailers, .responseTrailers, .streamError, .webSocketOpen, .webSocketClose:
            break
        }
    }

    private mutating func payload(_ bytes: [UInt8], count: Int) {
        byteCount = count
        retainedBytes = bytes.count
        if !bytes.isEmpty { payloadBase64 = Data(bytes).base64EncodedString() }
    }
}

extension CaptureEvent {
    fileprivate var outputSummary: (String, UUID) {
        switch self {
        case .requestHead(let head): ("requestHead", head.id)
        case .requestBodyChunk(let id, _, _): ("requestBodyChunk", id)
        case .requestTrailers(let id, _): ("requestTrailers", id)
        case .requestEnd(let id, _): ("requestEnd", id)
        case .responseHead(let head): ("responseHead", head.requestID)
        case .responseBodyChunk(let id, _, _): ("responseBodyChunk", id)
        case .responseTrailers(let id, _): ("responseTrailers", id)
        case .responseEnd(let id, _): ("responseEnd", id)
        case .streamError(let id, _): ("streamError", id)
        case .webSocketOpen(let id, _, _): ("webSocketOpen", id)
        case .webSocketFrame(let frame): ("webSocketFrame", frame.connectionID)
        case .webSocketClose(let id, _, _, _): ("webSocketClose", id)
        case .opaqueOpen(let flow): ("opaqueOpen", flow.id)
        case .opaqueData(let id, _, _, _, _): ("opaqueData", id)
        case .opaqueDirectionEnd(let id, _, _, _, _): ("opaqueDirectionEnd", id)
        case .opaqueClose(let id, _, _): ("opaqueClose", id)
        case .opaqueError(let id, _, _): ("opaqueError", id)
        case .connectionFailure(let failure): ("connectionFailure", failure.id)
        }
    }
}

final class CaptureOutput: CaptureEventSink {
    let delivery: BoundedDelivery?
    let asynchronousOutput: AsyncStdout?
    private let outputLock = OSAllocatedUnfairLock(initialState: false)

    init(asynchronous: Bool) throws {
        delivery = asynchronous ? BoundedDelivery() : nil
        asynchronousOutput = asynchronous ? try AsyncStdout() : nil
    }

    func consume() async {
        guard let delivery, let asynchronousOutput else { return }
        do {
            while let data = try await delivery.next() { try await asynchronousOutput.line(data) }
        } catch let error as DeliveryError {
            reportLoss(error)
        } catch {
            reportLoss(.outputFailed)
        }
    }

    func receive(_ event: CaptureEvent) {
        if let delivery, Self.payloadSize(event) > BoundedDelivery.maximumBytes {
            reportLoss(.overflow)
            return
        }
        do {
            let data = try JSONEncoder().encode(OutputRecord(event))
            if let delivery {
                delivery.offer(data)
                if let failure = delivery.failure { reportLoss(failure) }
            } else {
                write(data)
            }
        } catch {
            reportLoss(.encodingFailed)
        }
    }

    func emit(_ record: OutputRecord) {
        do {
            let data = try JSONEncoder().encode(record)
            if let delivery {
                delivery.offer(data)
                if let failure = delivery.failure { reportLoss(failure) }
            } else {
                write(data)
            }
        } catch {
            reportLoss(.encodingFailed)
        }
    }

    func write(_ data: Data) {
        outputLock.withLock { lost in
            guard !lost else { return }
            Console.line(data)
        }
    }

    func reportLoss(_ error: DeliveryError) {
        delivery?.fail(error)
        outputLock.withLock { lost in
            guard !lost else { return }
            lost = true
            if let asynchronousOutput {
                asynchronousOutput.reportLoss(delivery?.failure ?? error)
                return
            }
            let line = "{\"type\":\"deliveryLoss\",\"reason\":\"\(error.rawValue)\",\"allActiveCapturesIncomplete\":true}"
            Console.line(Data(line.utf8))
        }
    }

    func finishDelivery(cancel: Bool) {
        if cancel {
            reportLoss(.cancelled)
        } else {
            delivery?.finish()
            asynchronousOutput?.beginDrain()
        }
    }

    func complete() async throws {
        if let asynchronousOutput {
            if delivery?.failure == nil {
                do {
                    try await asynchronousOutput.line(JSONEncoder().encode(OutputRecord(type: "stopped")))
                } catch let failure as DeliveryError {
                    reportLoss(failure)
                } catch {
                    reportLoss(.outputFailed)
                }
            }
            try asynchronousOutput.restoreFlags()
        } else {
            emit(OutputRecord(type: "stopped"))
        }
    }

    private static func payloadSize(_ event: CaptureEvent) -> Int {
        switch event {
        case .requestBodyChunk(_, let bytes, _), .responseBodyChunk(_, let bytes, _):
            return bytes.count
        case .opaqueData(_, _, _, let bytes, _):
            return bytes.count
        case .webSocketFrame(let frame):
            return frame.bytes.count
        default:
            return 0
        }
    }
}

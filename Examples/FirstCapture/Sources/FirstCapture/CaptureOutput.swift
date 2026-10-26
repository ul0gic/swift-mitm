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
        case let .requestHead(head):
            method = String(head.method.prefix(256))
            authority = String(head.authority.prefix(256))
        case let .responseHead(head):
            status = head.status
        case let .requestBodyChunk(_, bytes, count), let .responseBodyChunk(_, bytes, count):
            payload(bytes, count: count)
        case let .requestEnd(_, isTruncated), let .responseEnd(_, isTruncated):
            truncated = isTruncated
        case let .webSocketFrame(frame):
            direction = frame.direction.rawValue
            payload(frame.bytes, count: frame.byteCount)
            truncated = frame.truncated
        case let .opaqueOpen(flow):
            authority = String(flow.target.logicalAuthority.prefix(256))
            reason = flow.reason.rawValue
        case let .opaqueData(_, _, flowDirection, bytes, count):
            direction = flowDirection.rawValue
            payload(bytes, count: count)
        case let .opaqueDirectionEnd(_, _, flowDirection, count, isTruncated):
            direction = flowDirection.rawValue
            byteCount = count
            truncated = isTruncated
        case let .opaqueClose(_, _, closeReason):
            reason = closeReason.rawValue
        case let .opaqueError(_, _, failureReason):
            reason = failureReason.rawValue
        case let .connectionFailure(failure):
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
        if !bytes.isEmpty {
            payloadBase64 = Data(bytes).base64EncodedString()
        }
    }
}

private extension CaptureEvent {
    var outputSummary: (String, UUID) {
        switch self {
        case let .requestHead(head): ("requestHead", head.id)
        case let .requestBodyChunk(id, _, _): ("requestBodyChunk", id)
        case let .requestTrailers(id, _): ("requestTrailers", id)
        case let .requestEnd(id, _): ("requestEnd", id)
        case let .responseHead(head): ("responseHead", head.requestID)
        case let .responseBodyChunk(id, _, _): ("responseBodyChunk", id)
        case let .responseTrailers(id, _): ("responseTrailers", id)
        case let .responseEnd(id, _): ("responseEnd", id)
        case let .streamError(id, _): ("streamError", id)
        case let .webSocketOpen(id, _, _): ("webSocketOpen", id)
        case let .webSocketFrame(frame): ("webSocketFrame", frame.connectionID)
        case let .webSocketClose(id, _, _, _): ("webSocketClose", id)
        case let .opaqueOpen(flow): ("opaqueOpen", flow.id)
        case let .opaqueData(id, _, _, _, _): ("opaqueData", id)
        case let .opaqueDirectionEnd(id, _, _, _, _): ("opaqueDirectionEnd", id)
        case let .opaqueClose(id, _, _): ("opaqueClose", id)
        case let .opaqueError(id, _, _): ("opaqueError", id)
        case let .connectionFailure(failure): ("connectionFailure", failure.id)
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
                if let failure = delivery.failure {
                    reportLoss(failure)
                }
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
                if let failure = delivery.failure {
                    reportLoss(failure)
                }
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
        case let .requestBodyChunk(_, bytes, _), let .responseBodyChunk(_, bytes, _):
            return bytes.count
        case let .opaqueData(_, _, _, bytes, _):
            return bytes.count
        case let .webSocketFrame(frame):
            return frame.bytes.count
        default:
            return 0
        }
    }
}

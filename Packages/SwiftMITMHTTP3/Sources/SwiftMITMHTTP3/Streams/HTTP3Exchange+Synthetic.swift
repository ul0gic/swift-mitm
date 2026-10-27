import Foundation
import HTTPTypes
import NIOCore
import NIOQUICHelpers
import SwiftMITM

extension HTTP3Exchange {
    func syntheticResponse(status: Int, body: Data, decision: HTTPControlDecision, flow: HTTP3Flow) throws {
        let fields = flow.isRequest ? (decision.syntheticResponseHeaders ?? []) : decision.headers
        let lease = try transport.resources.reserve(HTTP3Fields.encodedReservation(fields), kind: .forwarding)
        let head = HTTPResponse(status: HTTPResponse.Status(code: status), headerFields: try HTTP3Fields.encode(fields))
        if flow.isRequest {
            if let original = request.head {
                captureHead(original, fields: decision.headers, representation: .effective)
                transport.controlEventSink?.receive(.effectiveHead(
                    requestID: id, phase: .request, headers: decision.headers, appliedRuleIDs: decision.appliedRuleIDs
                ))
            }
            stopInput(request)
            request.outputEnded = true
            captureEnd(request, representation: .effective, truncated: true)
            response.decision = decision
            response.work = request.work
            response.inputClosed = true
            if let fault = try decision.selectedFault(direction: .response) {
                response.fault = HTTPFaultSchedule(fault: fault, startNanoseconds: NIODeadline.now().uptimeNanoseconds)
            }
        } else {
            if !response.inputEnded, !originalFailed {
                terminateOriginal("HTTP control replaced incomplete origin representation")
                response.inputEnded = true
            }
            stopInput(response)
        }
        response.inputEnded = true
        response.generatedBody = body
        response.generatedOffset = 0
        response.generatedEnd = HTTP3HeldMessage(message: .response(.end(nil)), lease: nil)
        try enqueue(.response(.head(head)), lease: lease, flow: response)
    }

    func stopInput(_ flow: HTTP3Flow) {
        flow.discardInput = true
        if !flow.inputEnded {
            flow.inputEnded = true
            captureEnd(flow, representation: .original, truncated: true)
        }
        flow.pendingBytes -= flow.input.reduce(0) { $0 + ($1.message.body?.readableBytes ?? 0) }
        flow.input.removeAll()
        observePending()
        guard let source = flow.source, let code = QUICApplicationErrorCode(0x10C) else { return }
        source.triggerUserOutboundEvent(QUICStopSendingEvent(code: code), promise: nil)
    }
}

import Foundation
import NIOCore
import NIOHTTP1
import SwiftMITMHTTP1Codec

extension ExplicitHTTPProxyHandler {
    final class Exchange {
        let id: UUID
        let head: HTTPRequestHead
        var target: HTTPForwardTarget?
        let originalTarget: HTTPForwardTarget?
        var effectiveHead: HTTPRequestHead
        var controlSnapshot: HTTPControlConfiguration?
        var controlReservation: HTTPControlByteReservation?
        var admissionReservation: ResourceReservation?
        var controlReady = true
        var controlCollectingBody = false
        var controlBody = Data()
        var controlMaximumBody = 0
        var controlTask: Task<Void, Never>?
        var responseControlHead: HTTPResponseHead?
        var responseControlReservation: HTTPControlByteReservation?
        var responseControlBody = Data()
        var responseControlMaximum = 0
        var responseControlCollecting = false
        var responseControlEnded = false
        var responseControlTrailers: HTTPHeaders?
        var requestControlTrailers: HTTPHeaders?
        var requestFault: HTTPFault?
        var responseFault: HTTPFault?
        var requestFaultWriter: HTTPControlFaultWriter?
        var responseFaultWriter: HTTPControlFaultWriter?
        var pendingByteCount = 0
        var upstreamHTTP2 = false
        var controlAdmissionReservation: ResourceReservation?
        var responseControlAdmissionReservation: ResourceReservation?
        var pendingBodyDecision: HTTPControlDecision?
        var pendingResponseBodyDecision: HTTPControlDecision?
        var syntheticDecision: HTTPControlDecision?
        var breakpointEditReservation: HTTPControlByteReservation?
        var breakpointEditAdmissionReservation: ResourceReservation?
        var effectiveRequestPublished = false
        var responseControlRuleIDs: [String] = []
        var responseDecoder: BytePreservingHTTPResponseDecoder?
        var reusableFraming = false
        var reusePoisoned = false
        var finishing = false
        var completedExchanges = 0
        let requestKeepAlive: Bool
        let timestamp = Date()
        var pending: [HTTPServerRequestPart] = []
        var requestBody: CaptureBodyBuffer
        var responseBody: CaptureBodyBuffer
        var requestEnded = false
        var responseStarted = false
        var responseEnded = false
        var earlyResponse = false
        var upgrading = false
        var permessageDeflate = false
        var published = false
        var originKeepAlive: Bool?
        var responseConnectionTokens: Set<String> = []

        init(id: UUID, head: HTTPRequestHead, target: HTTPForwardTarget?, bodyLimit: Int, requestKeepAlive: Bool) {
            self.id = id
            self.head = head
            effectiveHead = head
            self.target = target
            originalTarget = target
            self.requestKeepAlive = requestKeepAlive
            requestBody = CaptureBodyBuffer(limit: bodyLimit)
            responseBody = CaptureBodyBuffer(limit: bodyLimit)
        }
    }

}

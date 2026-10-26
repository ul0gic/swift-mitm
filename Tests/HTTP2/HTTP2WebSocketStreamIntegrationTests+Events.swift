import Foundation

@testable import SwiftMITM

extension HTTP2WebSocketStreamIntegrationTests {
    func eventKinds(_ events: [CaptureEvent]) -> [String] {
        events.compactMap { event in
            switch event {
            case .requestHead: "request"
            case .responseHead(let head): "response:\(head.status)"
            case .requestBodyChunk: "requestBody"
            case .responseBodyChunk: "responseBody"
            case .requestEnd: "requestEnd"
            case .responseEnd: "responseEnd"
            case .webSocketOpen: "open"
            case .webSocketFrame(let frame): "frame:\(frame.opcode)"
            case .webSocketClose: "close"
            default: nil
            }
        }
    }

    func webSocketFrames(_ events: [CaptureEvent]) -> [CapturedWebSocketFrame] {
        events.compactMap { event in
            guard case .webSocketFrame(let frame) = event else { return nil }
            return frame
        }
    }

    func responseOpenUsesPermessageDeflate(_ events: [CaptureEvent]) -> Bool {
        events.contains { event in
            guard case .webSocketOpen(_, _, let permessageDeflate) = event else { return false }
            return permessageDeflate
        }
    }

    func hasHTTPBodyEvent(_ events: [CaptureEvent]) -> Bool {
        events.contains { event in
            if case .requestBodyChunk = event {
                return true
            }
            if case .responseBodyChunk = event {
                return true
            }
            return false
        }
    }

    func hasWebSocketEvent(_ events: [CaptureEvent]) -> Bool {
        events.contains { event in
            switch event {
            case .webSocketOpen, .webSocketFrame, .webSocketClose: true
            default: false
            }
        }
    }

    func streamErrorCount(_ events: [CaptureEvent]) -> Int {
        streamErrorIDs(events).count
    }

    func streamErrorIDs(_ events: [CaptureEvent]) -> [UUID] {
        events.compactMap { event in
            guard case .streamError(let requestID, _) = event else { return nil }
            return requestID
        }
    }

    func webSocketCloseCount(_ events: [CaptureEvent]) -> Int {
        events.count { event in
            if case .webSocketClose = event {
                return true
            }
            return false
        }
    }

    func webSocketOpenIDs(_ events: [CaptureEvent]) -> [UUID] {
        events.compactMap { event in
            guard case .webSocketOpen(let connectionID, _, _) = event else { return nil }
            return connectionID
        }
    }
}

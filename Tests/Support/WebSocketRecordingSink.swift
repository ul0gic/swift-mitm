import Foundation
import NIOConcurrencyHelpers

import SwiftMITM

final class WebSocketRecordingSink: CaptureEventSink, @unchecked Sendable {
    enum EventKind: Equatable {
        case requestHead
        case requestBody
        case requestTrailers
        case requestEnd
        case responseHead
        case responseBody
        case responseTrailers
        case responseEnd
        case streamError
        case webSocketOpen
        case webSocketFrame(direction: String, opcode: UInt8)
        case webSocketClose
    }

    struct RequestHead {
        let id: UUID
        let scheme: String
        let authority: String
        let method: String
        let path: String
        let version: String
        let headers: [HTTPHeaderField]
    }

    struct ResponseHead {
        let requestID: UUID
        let status: Int
        let version: String
        let headers: [HTTPHeaderField]
    }

    struct Open {
        let connectionID: UUID
        let permessageDeflate: Bool
    }

    struct Close {
        let connectionID: UUID
        let code: Int?
        let reason: String?
    }

    struct Snapshot {
        var requestHeads: [RequestHead] = []
        var responseHeads: [ResponseHead] = []
        var opens: [Open] = []
        var frames: [CapturedWebSocketFrame] = []
        var closes: [Close] = []
        var eventKinds: [EventKind] = []
    }

    private let storage = NIOLockedValueBox(Snapshot())

    var snapshot: Snapshot { storage.withLockedValue { $0 } }

    func receive(_ event: CaptureEvent) {
        storage.withLockedValue { snapshot in
            recordRequest(event, in: &snapshot)
            recordResponse(event, in: &snapshot)
            recordWebSocket(event, in: &snapshot)
        }
    }

    private func recordRequest(_ event: CaptureEvent, in snapshot: inout Snapshot) {
        switch event {
        case .requestHead(let head):
            snapshot.requestHeads.append(RequestHead(
                id: head.id,
                scheme: head.scheme,
                authority: head.authority,
                method: head.method,
                path: head.path,
                version: head.version.rawValue,
                headers: head.headers
            ))
            snapshot.eventKinds.append(.requestHead)
        case .requestBodyChunk:
            snapshot.eventKinds.append(.requestBody)
        case .requestTrailers:
            snapshot.eventKinds.append(.requestTrailers)
        case .requestEnd:
            snapshot.eventKinds.append(.requestEnd)
        default:
            break
        }
    }

    private func recordResponse(_ event: CaptureEvent, in snapshot: inout Snapshot) {
        switch event {
        case .responseHead(let head):
            snapshot.responseHeads.append(ResponseHead(
                requestID: head.requestID,
                status: head.status,
                version: head.version.rawValue,
                headers: head.headers
            ))
            snapshot.eventKinds.append(.responseHead)
        case .responseBodyChunk:
            snapshot.eventKinds.append(.responseBody)
        case .responseTrailers:
            snapshot.eventKinds.append(.responseTrailers)
        case .responseEnd:
            snapshot.eventKinds.append(.responseEnd)
        case .streamError:
            snapshot.eventKinds.append(.streamError)
        default:
            break
        }
    }

    private func recordWebSocket(_ event: CaptureEvent, in snapshot: inout Snapshot) {
        switch event {
        case let .webSocketOpen(connectionID, _, permessageDeflate):
            snapshot.opens.append(Open(connectionID: connectionID, permessageDeflate: permessageDeflate))
            snapshot.eventKinds.append(.webSocketOpen)
        case .webSocketFrame(let frame):
            snapshot.frames.append(frame)
            snapshot.eventKinds.append(.webSocketFrame(
                direction: frame.direction.rawValue,
                opcode: frame.opcode.rawValue
            ))
        case let .webSocketClose(connectionID, _, code, reason):
            snapshot.closes.append(Close(connectionID: connectionID, code: code, reason: reason))
            snapshot.eventKinds.append(.webSocketClose)
        default:
            break
        }
    }
}

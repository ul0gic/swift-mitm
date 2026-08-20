import Foundation

@testable import SwiftMITM

final class Phase4CaptureSink: CaptureEventSink, TransparentIngressStageObserver, @unchecked Sendable {
    enum EventKind: Equatable {
        case requestHead
        case requestBody
        case requestEnd
        case responseHead
        case responseBody
        case responseEnd
        case streamError
        case webSocketOpen
        case webSocketFrame
        case webSocketClose
        case opaqueOpen
        case opaqueData
        case opaqueDirectionEnd
        case opaqueClose
        case opaqueError
        case connectionFailure
    }

    struct OpaqueData: Equatable {
        let flowID: UUID
        let direction: OpaqueFlowDirection
        let bytes: [UInt8]
        let byteCount: Int
    }

    struct OpaqueDirectionEnd: Equatable {
        let flowID: UUID
        let direction: OpaqueFlowDirection
        let byteCount: Int
        let truncated: Bool
    }

    struct OpaqueClose: Equatable {
        let flowID: UUID
        let reason: OpaqueFlowCloseReason
    }

    struct OpaqueError: Equatable {
        let flowID: UUID
        let reason: CapturedConnectionFailureReason
    }

    struct WebSocketOpen: Equatable {
        let connectionID: UUID
        let permessageDeflate: Bool
    }

    struct WebSocketClose: Equatable {
        let connectionID: UUID
        let code: Int?
        let reason: String?
    }

    struct BodyChunk: Equatable {
        let requestID: UUID
        let bytes: [UInt8]
        let byteCount: Int
    }

    struct MessageEnd: Equatable {
        let requestID: UUID
        let truncated: Bool
    }

    struct Snapshot {
        var eventKinds: [EventKind] = []
        var requestHeads: [CapturedRequestHead] = []
        var requestBodies: [BodyChunk] = []
        var requestEnds: [MessageEnd] = []
        var responseHeads: [CapturedResponseHead] = []
        var responseBodies: [BodyChunk] = []
        var responseEnds: [MessageEnd] = []
        var webSocketOpens: [WebSocketOpen] = []
        var webSocketFrames: [CapturedWebSocketFrame] = []
        var webSocketCloses: [WebSocketClose] = []
        var opaqueFlows: [CapturedOpaqueFlow] = []
        var opaqueData: [OpaqueData] = []
        var opaqueDirectionEnds: [OpaqueDirectionEnd] = []
        var opaqueCloses: [OpaqueClose] = []
        var opaqueErrors: [OpaqueError] = []
        var connectionFailures: [CapturedConnectionFailure] = []
    }

    private let condition = NSCondition()
    private var storage = Snapshot()
    private var proxyHeaderPendingCount = 0
    private var classificationPendingCount = 0
    private var opaqueBridgeReadyCount = 0

    var snapshot: Snapshot {
        condition.withLock { storage }
    }

    func receive(_ event: CaptureEvent) {
        condition.withLock {
            recordHTTP(event)
            recordWebSocket(event)
            recordOpaque(event)
            condition.broadcast()
        }
    }

    func didEnterTransparentIngressStage(_ stage: TransparentIngressStage) {
        condition.withLock {
            switch stage {
            case .proxyHeaderPending:
                proxyHeaderPendingCount += 1
            case .classificationPending:
                classificationPendingCount += 1
            case .opaqueBridgeReady:
                opaqueBridgeReadyCount += 1
            }
            condition.broadcast()
        }
    }

    func waitForProxyHeaderPending() throws {
        try waitForTransparentIngressStage(.proxyHeaderPending)
    }

    func waitForClassificationPending() throws {
        try waitForTransparentIngressStage(.classificationPending)
    }

    func waitForOpaqueBridgeReady() throws {
        try waitForTransparentIngressStage(.opaqueBridgeReady)
    }

    func wait(
        timeout: TimeInterval = 2,
        until predicate: (Snapshot) -> Bool
    ) throws -> Snapshot {
        let deadline = Date().addingTimeInterval(timeout)
        return try condition.withLock {
            while !predicate(storage) {
                guard condition.wait(until: deadline) else {
                    throw Phase2FixtureError.deadlineExceeded
                }
            }
            return storage
        }
    }

    private func recordHTTP(_ event: CaptureEvent) {
        switch event {
        case .requestHead(let head):
            storage.requestHeads.append(head)
            storage.eventKinds.append(.requestHead)
        case let .requestBodyChunk(requestID, bytes, byteCount):
            storage.requestBodies.append(.init(requestID: requestID, bytes: bytes, byteCount: byteCount))
            storage.eventKinds.append(.requestBody)
        case let .requestEnd(requestID, truncated):
            storage.requestEnds.append(.init(requestID: requestID, truncated: truncated))
            storage.eventKinds.append(.requestEnd)
        case .responseHead(let head):
            storage.responseHeads.append(head)
            storage.eventKinds.append(.responseHead)
        case let .responseBodyChunk(requestID, bytes, byteCount):
            storage.responseBodies.append(.init(requestID: requestID, bytes: bytes, byteCount: byteCount))
            storage.eventKinds.append(.responseBody)
        case let .responseEnd(requestID, truncated):
            storage.responseEnds.append(.init(requestID: requestID, truncated: truncated))
            storage.eventKinds.append(.responseEnd)
        case .streamError:
            storage.eventKinds.append(.streamError)
        default:
            break
        }
    }

    private func waitForTransparentIngressStage(_ stage: TransparentIngressStage) throws {
        let deadline = Date().addingTimeInterval(2)
        try condition.withLock {
            while !hasEnteredTransparentIngressStage(stage) {
                guard condition.wait(until: deadline) else {
                    throw Phase2FixtureError.deadlineExceeded
                }
            }
        }
    }

    private func hasEnteredTransparentIngressStage(_ stage: TransparentIngressStage) -> Bool {
        switch stage {
        case .proxyHeaderPending:
            proxyHeaderPendingCount > 0
        case .classificationPending:
            classificationPendingCount > 0
        case .opaqueBridgeReady:
            opaqueBridgeReadyCount > 0
        }
    }

    private func recordWebSocket(_ event: CaptureEvent) {
        switch event {
        case let .webSocketOpen(connectionID, _, permessageDeflate):
            storage.webSocketOpens.append(.init(
                connectionID: connectionID,
                permessageDeflate: permessageDeflate
            ))
            storage.eventKinds.append(.webSocketOpen)
        case .webSocketFrame(let frame):
            storage.webSocketFrames.append(frame)
            storage.eventKinds.append(.webSocketFrame)
        case let .webSocketClose(connectionID, _, code, reason):
            storage.webSocketCloses.append(.init(connectionID: connectionID, code: code, reason: reason))
            storage.eventKinds.append(.webSocketClose)
        default:
            break
        }
    }

    private func recordOpaque(_ event: CaptureEvent) {
        switch event {
        case .opaqueOpen(let flow):
            storage.opaqueFlows.append(flow)
            storage.eventKinds.append(.opaqueOpen)
        case let .opaqueData(flowID, _, direction, bytes, byteCount):
            storage.opaqueData.append(.init(
                flowID: flowID,
                direction: direction,
                bytes: bytes,
                byteCount: byteCount
            ))
            storage.eventKinds.append(.opaqueData)
        case let .opaqueDirectionEnd(flowID, _, direction, byteCount, truncated):
            storage.opaqueDirectionEnds.append(.init(
                flowID: flowID,
                direction: direction,
                byteCount: byteCount,
                truncated: truncated
            ))
            storage.eventKinds.append(.opaqueDirectionEnd)
        case let .opaqueClose(flowID, _, reason):
            storage.opaqueCloses.append(.init(flowID: flowID, reason: reason))
            storage.eventKinds.append(.opaqueClose)
        case let .opaqueError(flowID, _, reason):
            storage.opaqueErrors.append(.init(flowID: flowID, reason: reason))
            storage.eventKinds.append(.opaqueError)
        case .connectionFailure(let failure):
            storage.connectionFailures.append(failure)
            storage.eventKinds.append(.connectionFailure)
        default:
            break
        }
    }
}

private extension NSCondition {
    func withLock<Value>(_ body: () throws -> Value) rethrows -> Value {
        lock()
        defer { unlock() }
        return try body()
    }
}

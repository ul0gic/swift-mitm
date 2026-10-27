import Foundation
import SwiftMITM

public enum InspectionDiagnostic: Sendable {
    case capacityExceeded
    case unavailableInput
    case incompleteTransport
    case unsupportedCoding(String)
    case malformedCoding(ContentDecoderError)
    case malformedSSE(SSEParserError)
    case unfinishedSSERecord
    case schemaUnavailable
    case schemaFailure(ProtobufInspectionError)
    case malformedWebSocket(WebSocketMessageError)
    case stopped
}

public struct InspectionEvent: Sendable {
    public enum Payload: Sendable {
        case head([HTTPHeaderField])
        case trailers([HTTPHeaderField])
        case decodedBody([UInt8])
        case sse(SSEEvent)
        case grpc(GRPCStreamEvent)
        case protobuf(messageName: String, schemaDigest: String, fields: [ProtobufFieldOccurrence])
        case webSocketChunk([UInt8])
        case webSocketMessage(WebSocketMessageSummary)
        case diagnostic(InspectionDiagnostic)
        case end
    }

    public let requestID: UUID
    public let direction: HTTPObservationDirection
    public let representation: HTTPRepresentationKind
    public let sequence: UInt64
    public let wireRange: Range<UInt64>
    public let decodedRange: Range<UInt64>
    public let codingChain: [ContentCoding]
    public let payload: Payload
}

public protocol InspectionEventSink: Sendable {
    func receive(_ event: InspectionEvent)
}

public struct InspectionDrainResult: Sendable {
    public let drained: Bool
    public let queuedItems: Int
    public let retainedBytes: Int
    public let callbackInFlight: Bool
    public let observationLossDetected: Bool
}

import Foundation

public enum HTTPObservationDirection: Sendable, Hashable {
    case request
    case response
}

public enum HTTPRepresentationKind: Sendable, Hashable {
    case original
    case effective
}

public struct HTTPBodyObservation: Sendable {
    public let requestID: UUID
    public let direction: HTTPObservationDirection
    public let representation: HTTPRepresentationKind

    public init(
        requestID: UUID,
        direction: HTTPObservationDirection,
        representation: HTTPRepresentationKind = .original
    ) {
        self.requestID = requestID
        self.direction = direction
        self.representation = representation
    }
}

public protocol HTTPObservationSink: CaptureEventSink {
    func receive(_ event: CaptureEvent, representation: HTTPRepresentationKind)
    /// Bytes are borrowed for this call; reserve bounded storage before copying them.
    func receiveBody(_ observation: HTTPBodyObservation, bytes: UnsafeRawBufferPointer)
    func bodyUnavailable(_ observation: HTTPBodyObservation)
}

final class ObservationCaptureSink: CaptureEventSink {
    let capture: any CaptureEventSink
    let observation: any HTTPObservationSink

    init(capture: any CaptureEventSink, observation: any HTTPObservationSink) {
        self.capture = capture
        self.observation = observation
    }

    func receive(_ event: CaptureEvent) {
        observation.receive(event, representation: .original)
        capture.receive(event)
    }

    func receiveEffective(_ event: CaptureEvent) {
        observation.receive(event, representation: .effective)
    }

    func receiveBody<Bytes: Collection>(
        requestID: UUID,
        direction: HTTPObservationDirection,
        representation: HTTPRepresentationKind = .original,
        bytes: Bytes
    ) where Bytes.Element == UInt8 {
        let context = HTTPBodyObservation(requestID: requestID, direction: direction, representation: representation)
        let contiguous: Void? = bytes.withContiguousStorageIfAvailable { buffer in
            observation.receiveBody(context, bytes: UnsafeRawBufferPointer(buffer))
        }
        if case .some = contiguous {
            return
        }
        var start = bytes.startIndex
        while start != bytes.endIndex {
            let end = bytes.index(start, offsetBy: 65_536, limitedBy: bytes.endIndex) ?? bytes.endIndex
            let slice = Array(bytes[start..<end])
            slice.withUnsafeBytes { observation.receiveBody(context, bytes: $0) }
            start = end
        }
    }
}

extension ObservationCaptureSink: WebSocketObservationSink {
    func webSocketFrameStarted(
        connectionID: UUID, direction: WebSocketDirection, opcode: WebSocketOpcode,
        fin: Bool, compressed: Bool, byteCount: Int
    ) {
        (observation as? any WebSocketObservationSink)?.webSocketFrameStarted(
            connectionID: connectionID, direction: direction, opcode: opcode,
            fin: fin, compressed: compressed, byteCount: byteCount
        )
    }

    func webSocketFrameBytes(
        connectionID: UUID, direction: WebSocketDirection, bytes: UnsafeRawBufferPointer
    ) {
        (observation as? any WebSocketObservationSink)?.webSocketFrameBytes(
            connectionID: connectionID, direction: direction, bytes: bytes
        )
    }

    func webSocketFrameEnded(connectionID: UUID, direction: WebSocketDirection) {
        (observation as? any WebSocketObservationSink)?.webSocketFrameEnded(
            connectionID: connectionID, direction: direction
        )
    }
}

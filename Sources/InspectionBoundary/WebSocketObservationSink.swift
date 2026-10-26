import Foundation

public protocol WebSocketObservationSink: CaptureEventSink {
    func webSocketFrameStarted(
        connectionID: UUID,
        direction: WebSocketDirection,
        opcode: WebSocketOpcode,
        fin: Bool,
        compressed: Bool,
        byteCount: Int
    )
    /// Bytes are unmasked and borrowed only for this call.
    func webSocketFrameBytes(
        connectionID: UUID,
        direction: WebSocketDirection,
        bytes: UnsafeRawBufferPointer
    )
    func webSocketFrameEnded(connectionID: UUID, direction: WebSocketDirection)
}

import Foundation

final class WebSocketCaptureSession {
    private enum Lifecycle {
        case prepared
        case open
        case closed
    }

    private struct ActiveSession {
        let id: UUID
        let sink: CaptureEventSink
        let clientDecoder: WebSocketFrameDecoder
        let serverDecoder: WebSocketFrameDecoder
        var permessageDeflate = false
        var lifecycle = Lifecycle.prepared
        var clientClosed = false
        var serverClosed = false
        var closeCode: Int?
        var closeReason: String?
    }

    private var active: ActiveSession?

    var isClosed: Bool {
        active?.lifecycle == .closed
    }

    func prepare(id: UUID, sink: CaptureEventSink, captureLimit: Int) {
        guard active == nil else { return }
        active = ActiveSession(
            id: id,
            sink: sink,
            clientDecoder: WebSocketFrameDecoder(captureLimit: captureLimit),
            serverDecoder: WebSocketFrameDecoder(captureLimit: captureLimit)
        )
        if let observer = sink as? any WebSocketObservationSink, let session = active {
            for (direction, decoder) in [
                (WebSocketDirection.clientToServer, session.clientDecoder),
                (WebSocketDirection.serverToClient, session.serverDecoder),
            ] {
                decoder.frameStarted = { opcode, fin, compressed, count in
                    observer.webSocketFrameStarted(
                        connectionID: id,
                        direction: direction,
                        opcode: opcode,
                        fin: fin,
                        compressed: compressed,
                        byteCount: count
                    )
                }
                decoder.frameBytes = { bytes in
                    observer.webSocketFrameBytes(connectionID: id, direction: direction, bytes: bytes)
                }
                decoder.frameEnded = {
                    observer.webSocketFrameEnded(connectionID: id, direction: direction)
                }
            }
        }
    }

    func open(id: UUID, sink: CaptureEventSink, captureLimit: Int, permessageDeflate: Bool) {
        prepare(id: id, sink: sink, captureLimit: captureLimit)
        guard var session = active, session.id == id, session.lifecycle == .prepared else { return }
        session.permessageDeflate = permessageDeflate
        session.lifecycle = .open
        active = session
        session.sink.receive(
            .webSocketOpen(
                connectionID: session.id,
                timestamp: Date(),
                permessageDeflate: session.permessageDeflate
            )
        )
    }

    @discardableResult
    func capture<Bytes: Collection>(
        _ bytes: Bytes,
        direction: WebSocketDirection
    ) -> Bool where Bytes.Element == UInt8 {
        guard let session = active, session.lifecycle != .closed else { return true }
        let decoder = direction == .clientToServer ? session.clientDecoder : session.serverDecoder
        return decoder.decode(bytes, shouldContinue: { [self] in active?.lifecycle != .closed }) { [self] frame in
            emit(frame, direction: direction)
        }
    }

    func close(code: Int? = nil, reason: String? = nil) {
        guard var session = active, session.lifecycle != .closed else { return }
        session.lifecycle = .closed
        active = session
        session.sink.receive(
            .webSocketClose(
                connectionID: session.id,
                timestamp: Date(),
                code: code ?? session.closeCode,
                reason: reason ?? session.closeReason
            )
        )
    }

    func isAtFrameBoundary(direction: WebSocketDirection) -> Bool {
        guard let session = active else { return true }
        let decoder = direction == .clientToServer ? session.clientDecoder : session.serverDecoder
        return decoder.isAtFrameBoundary
    }

    private func emit(_ frame: WebSocketFrameDecoder.Frame, direction: WebSocketDirection) {
        guard var session = active, session.lifecycle != .closed else { return }
        if frame.opcode == .connectionClose {
            if !session.clientClosed, !session.serverClosed {
                session.closeCode = frame.closeCode
                session.closeReason = frame.closeReason
            }
            switch direction {
            case .clientToServer:
                session.clientClosed = true
            case .serverToClient:
                session.serverClosed = true
            }
            active = session
        }
        session.sink.receive(
            .webSocketFrame(
                CapturedWebSocketFrame(
                    connectionID: session.id,
                    timestamp: Date(),
                    direction: direction,
                    opcode: frame.opcode,
                    fin: frame.fin,
                    compressed: frame.compressed,
                    bytes: frame.bytes,
                    byteCount: frame.byteCount,
                    truncated: frame.truncated,
                    closeCode: frame.closeCode,
                    closeReason: frame.closeReason
                )
            )
        )
        if session.clientClosed, session.serverClosed {
            close()
        }
    }
}

import Foundation
import NIOCore

final class OpaqueCaptureSession {
    private struct DirectionCapture {
        private var remaining: Int
        private(set) var byteCount = 0
        private(set) var retainedByteCount = 0
        private(set) var ended = false

        init(limit: Int) {
            remaining = max(0, limit)
        }

        mutating func capture(_ bytes: ByteBufferView) -> [UInt8]? {
            guard !ended else { return nil }
            let (newByteCount, overflow) = byteCount.addingReportingOverflow(bytes.count)
            byteCount = overflow ? Int.max : newByteCount
            let retained = Array(bytes.prefix(remaining))
            remaining -= retained.count
            retainedByteCount += retained.count
            return retained
        }

        mutating func end() -> Bool {
            guard !ended else { return false }
            ended = true
            return true
        }

        var truncated: Bool {
            retainedByteCount < byteCount
        }
    }

    private enum TerminalState {
        case open
        case closed
        case failed
    }

    private let flow: CapturedOpaqueFlow
    private let sink: CaptureEventSink
    private var clientToServer: DirectionCapture
    private var serverToClient: DirectionCapture
    private var opened = false
    private var terminalState = TerminalState.open

    init(flow: CapturedOpaqueFlow, sink: CaptureEventSink, captureByteLimit: Int = 0) {
        self.flow = flow
        self.sink = sink
        clientToServer = DirectionCapture(limit: captureByteLimit)
        serverToClient = DirectionCapture(limit: captureByteLimit)
    }

    func open() {
        guard !opened, terminalState == .open else { return }
        opened = true
        sink.receive(.opaqueOpen(flow))
    }

    func capture(_ buffer: ByteBuffer, direction: OpaqueFlowDirection) -> Bool {
        guard terminalState == .open else { return false }
        open()
        let bytes: [UInt8]?
        switch direction {
        case .clientToServer:
            bytes = clientToServer.capture(buffer.readableBytesView)
        case .serverToClient:
            bytes = serverToClient.capture(buffer.readableBytesView)
        }
        guard let bytes else { return false }
        sink.receive(.opaqueData(
            flowID: flow.id,
            timestamp: Date(),
            direction: direction,
            bytes: bytes,
            byteCount: buffer.readableBytes
        ))
        return true
    }

    func directionEnd(_ direction: OpaqueFlowDirection) {
        guard terminalState == .open else { return }
        open()
        let byteCount: Int
        let truncated: Bool
        let emitted: Bool
        switch direction {
        case .clientToServer:
            emitted = clientToServer.end()
            byteCount = clientToServer.byteCount
            truncated = clientToServer.truncated
        case .serverToClient:
            emitted = serverToClient.end()
            byteCount = serverToClient.byteCount
            truncated = serverToClient.truncated
        }
        if emitted {
            sink.receive(.opaqueDirectionEnd(
                flowID: flow.id,
                timestamp: Date(),
                direction: direction,
                byteCount: byteCount,
                truncated: truncated
            ))
        }
        if clientToServer.ended, serverToClient.ended {
            close(reason: .completed)
        }
    }

    func close(reason: OpaqueFlowCloseReason) {
        guard terminalState == .open else { return }
        open()
        terminalState = .closed
        sink.receive(.opaqueClose(flowID: flow.id, timestamp: Date(), reason: reason))
    }

    func fail(reason: CapturedConnectionFailureReason) {
        guard terminalState == .open else { return }
        open()
        terminalState = .failed
        sink.receive(.opaqueError(flowID: flow.id, timestamp: Date(), reason: reason))
    }
}

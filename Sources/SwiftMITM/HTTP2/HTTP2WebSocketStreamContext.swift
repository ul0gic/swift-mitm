import Foundation
import NIOCore
import NIOHPACK

final class HTTP2WebSocketStreamContext {
    private final class PairedStreams {
        weak var request: Channel?
        weak var response: Channel?

        init(request: Channel, response: Channel) {
            self.request = request
            self.response = response
        }

        func close() {
            request?.close(promise: nil)
            response?.close(promise: nil)
        }
    }

    private let requestID: UUID
    private let sink: CaptureEventSink
    private let captureLimit: Int
    private let extendedConnectEnabled: Bool
    private let session = WebSocketCaptureSession()
    private var coordinator = HTTP2ExtendedConnectStreamCoordinator()
    private var webSocketOpened = false
    private var pairedStreams: PairedStreams?
    private var pairedStreamsClosed = false

    init(
        requestID: UUID,
        sink: CaptureEventSink,
        captureLimit: Int,
        extendedConnectEnabled: Bool
    ) {
        self.requestID = requestID
        self.sink = sink
        self.captureLimit = captureLimit
        self.extendedConnectEnabled = extendedConnectEnabled
    }

    var state: HTTP2ExtendedConnectStreamState {
        coordinator.state
    }

    var requiresStreamTerminationForViolation: Bool {
        webSocketOpened || coordinator.state == .candidateExtendedConnect
    }

    var isWebSocketCaptureClosed: Bool {
        webSocketOpened && session.isClosed
    }

    func configurePairedStreams(request: Channel, response: Channel) {
        guard pairedStreams == nil else { return }
        pairedStreams = PairedStreams(request: request, response: response)
    }

    func receiveInitialRequest(
        headers: HPACKHeaders,
        endStream: Bool
    ) -> HTTP2ExtendedConnectStreamDisposition {
        let disposition = coordinator.receiveInitialRequest(
            headers: headers,
            endStream: endStream,
            extendedConnectEnabled: extendedConnectEnabled
        )
        if disposition == .awaitFinalResponse {
            session.prepare(id: requestID, sink: sink, captureLimit: captureLimit)
        }
        return disposition
    }

    func receiveResponse(
        status: Int,
        endStream: Bool
    ) -> HTTP2ExtendedConnectStreamDisposition {
        let disposition = coordinator.receiveResponse(status: status, endStream: endStream)
        closeIfEnded()
        return disposition
    }

    func openWebSocket(responseHeaders: HPACKHeaders) {
        guard coordinator.state == .acceptedWebSocket, !webSocketOpened else { return }
        webSocketOpened = true
        session.open(
            id: requestID,
            sink: sink,
            captureLimit: captureLimit,
            permessageDeflate: Self.negotiatesPermessageDeflate(responseHeaders)
        )
    }

    func receiveData(
        _ data: IOData,
        direction: HTTP2StreamDirection,
        endStream: Bool
    ) -> HTTP2ExtendedConnectStreamDisposition {
        let disposition = coordinator.receiveData(direction: direction, endStream: endStream)
        guard disposition == .forwardWebSocket else {
            closeIfEnded()
            return disposition
        }
        let webSocketDirection: WebSocketDirection = direction == .request ? .clientToServer : .serverToClient
        let valid: Bool
        switch data {
        case .byteBuffer(let buffer):
            valid = session.capture(buffer.readableBytesView, direction: webSocketDirection)
        case .fileRegion:
            valid = false
        }
        guard valid else {
            return coordinator.fail()
        }
        guard !endStream || session.isAtFrameBoundary(direction: webSocketDirection) else {
            return coordinator.fail()
        }
        if endStream, direction == .response {
            session.close()
        }
        closeIfEnded()
        return disposition
    }

    func end(_ direction: HTTP2StreamDirection) {
        coordinator.end(direction)
        closeIfEnded()
    }

    func terminate(failed: Bool) {
        if failed {
            _ = coordinator.fail()
            closePairedStreams()
        }
        if webSocketOpened {
            session.close()
        }
    }

    private func closePairedStreams() {
        guard !pairedStreamsClosed else { return }
        pairedStreamsClosed = true
        pairedStreams?.close()
    }

    private func closeIfEnded() {
        if coordinator.state == .ended, webSocketOpened {
            session.close()
        }
    }

    private static func negotiatesPermessageDeflate(_ headers: HPACKHeaders) -> Bool {
        headers.contains { name, value, _ in
            name.lowercased() == "sec-websocket-extensions"
                && value.lowercased().contains("permessage-deflate")
        }
    }
}

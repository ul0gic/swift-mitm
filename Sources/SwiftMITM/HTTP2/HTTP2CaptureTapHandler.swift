import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2

final class HTTP2StreamErrorState: Sendable {
    private let emitted = NIOLockedValueBox(false)

    func claim() -> Bool {
        emitted.withLockedValue { value in
            guard !value else { return false }
            value = true
            return true
        }
    }
}

final class HTTP2CaptureTapHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias InboundOut = HTTP2Frame.FramePayload

    enum Direction {
        case request
        case response
    }

    private enum Phase: Equatable {
        case awaitingHead
        case body
        case ended
        case failed
    }

    private static let errorMessage = "HTTP/2 capture stream invalid"

    private let direction: Direction
    private let requestID: UUID
    private let authority: String
    private let target: CapturedTarget?
    private let sink: CaptureEventSink
    private let errorState: HTTP2StreamErrorState
    private let streamContext: NIOLoopBound<HTTP2WebSocketStreamContext>?
    private var bodyBuffer: CaptureBodyBuffer
    private var phase: Phase = .awaitingHead

    init(
        direction: Direction,
        requestID: UUID,
        authority: String,
        target: CapturedTarget? = nil,
        sink: CaptureEventSink,
        captureBodyLimit: Int = 0,
        errorState: HTTP2StreamErrorState = HTTP2StreamErrorState(),
        streamContext: NIOLoopBound<HTTP2WebSocketStreamContext>? = nil
    ) {
        self.direction = direction
        self.requestID = requestID
        self.authority = authority
        self.target = target
        self.sink = sink
        self.bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
        self.errorState = errorState
        self.streamContext = streamContext
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let payload = unwrapInboundIn(data)
        let shouldForward: Bool
        switch payload {
        case .headers(let frame):
            shouldForward = processHeaders(frame.headers, endStream: frame.endStream)
        case .data(let frame):
            shouldForward = processData(frame.data, endStream: frame.endStream)
        case .rstStream:
            if streamContext?.value.isWebSocketCaptureClosed != true {
                failCapture(terminateStream: streamContext?.value.requiresStreamTerminationForViolation == true)
            }
            shouldForward = true
        default:
            shouldForward = true
        }
        if shouldForward {
            context.fireChannelRead(data)
        } else {
            context.close(promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if phase != .ended, phase != .failed,
           streamContext?.value.isWebSocketCaptureClosed != true {
            failCapture(terminateStream: streamContext?.value.requiresStreamTerminationForViolation == true)
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        if isResponseEndWindowUpdateError(error) || isPostResponseEndCancellation(error) {
            return
        }
        if streamContext?.value.isWebSocketCaptureClosed != true {
            failCapture(terminateStream: streamContext?.value.requiresStreamTerminationForViolation == true)
        }
        context.fireErrorCaught(error)
    }

    private func processHeaders(_ headers: HPACKHeaders, endStream: Bool) -> Bool {
        switch (direction, phase) {
        case (.request, .awaitingHead):
            return processInitialRequestHeaders(headers, endStream: endStream)
        case (.response, .awaitingHead):
            return processInitialResponseHeaders(headers, endStream: endStream)
        case (_, .body):
            return processTrailerHeaders(headers, endStream: endStream)
        case (_, .ended):
            failCapture(terminateStream: streamContext?.value.requiresStreamTerminationForViolation == true)
            return streamContext?.value.requiresStreamTerminationForViolation != true
        case (_, .failed):
            return streamContext?.value.requiresStreamTerminationForViolation != true
        }
    }

    private func processInitialRequestHeaders(_ headers: HPACKHeaders, endStream: Bool) -> Bool {
        if let streamContext,
           streamContext.value.receiveInitialRequest(headers: headers, endStream: endStream) == .streamError {
            failCapture(terminateStream: true)
            return false
        }
        guard isValidRequestHead(headers) else {
            failCapture()
            return true
        }
        emitRequestHead(headers)
        if endStream {
            emitEnd()
        } else {
            enterBody()
        }
        return true
    }

    private func processInitialResponseHeaders(_ headers: HPACKHeaders, endStream: Bool) -> Bool {
        guard let status = validResponseStatus(headers) else {
            let terminateStream = streamContext?.value.requiresStreamTerminationForViolation == true
            failCapture(terminateStream: terminateStream)
            return !terminateStream
        }
        let disposition = streamContext?.value.receiveResponse(status: status, endStream: endStream)
        if disposition == .streamError {
            failCapture(terminateStream: true)
            return false
        }
        guard status >= 200 else {
            guard !endStream else {
                let terminateStream = streamContext?.value.requiresStreamTerminationForViolation == true
                failCapture(terminateStream: terminateStream)
                return !terminateStream
            }
            emitResponseHead(headers, status: status)
            return true
        }
        emitResponseHead(headers, status: status)
        if disposition == .openWebSocket {
            streamContext?.value.openWebSocket(responseHeaders: headers)
        }
        if endStream {
            emitEnd()
        } else {
            enterBody()
        }
        return true
    }

    private func processTrailerHeaders(_ headers: HPACKHeaders, endStream: Bool) -> Bool {
        let terminateStream = streamContext?.value.requiresStreamTerminationForViolation == true
        guard !terminateStream else {
            failCapture(terminateStream: true)
            return false
        }
        guard !containsPseudoHeaders(headers), endStream else {
            failCapture()
            return true
        }
        emitTrailers(headers)
        streamContext?.value.end(streamDirection)
        emitEnd()
        return true
    }

    private func enterBody() {
        phase = .body
    }

    private func processData(_ data: IOData, endStream: Bool) -> Bool {
        guard phase == .body else {
            if phase != .failed {
                failCapture(terminateStream: streamContext?.value.requiresStreamTerminationForViolation == true)
            }
            return streamContext?.value.requiresStreamTerminationForViolation != true
        }
        if let streamContext {
            switch streamContext.value.receiveData(data, direction: streamDirection, endStream: endStream) {
            case .forwardHTTP:
                captureBody(data)
            case .forwardWebSocket:
                break
            case .streamError, .awaitFinalResponse, .openWebSocket:
                failCapture(terminateStream: true)
                return false
            }
        } else {
            captureBody(data)
        }
        if endStream {
            emitEnd()
        }
        return true
    }

    private func isValidRequestHead(_ headers: HPACKHeaders) -> Bool {
        let pseudoHeaders = pseudoHeaderPairs(headers)
        let names = pseudoHeaders.map(\.0)
        let allowed = Set([":method", ":scheme", ":authority", ":path", ":protocol"])
        guard Set(names).count == names.count, names.allSatisfy(allowed.contains) else { return false }
        return pseudoHeaders.count { $0.0 == ":method" } == 1
    }

    private func validResponseStatus(_ headers: HPACKHeaders) -> Int? {
        let pseudoHeaders = pseudoHeaderPairs(headers)
        guard pseudoHeaders.count == 1, pseudoHeaders[0].0 == ":status",
            pseudoHeaders[0].1.utf8.count == 3,
            let status = Int(pseudoHeaders[0].1), (100...599).contains(status), status != 101 else {
            return nil
        }
        return status
    }

    private func pseudoHeaderPairs(_ headers: HPACKHeaders) -> [(String, String)] {
        headers.compactMap { name, value, _ in name.hasPrefix(":") ? (name, value) : nil }
    }

    private func containsPseudoHeaders(_ headers: HPACKHeaders) -> Bool {
        headers.contains { name, _, _ in name.hasPrefix(":") }
    }

    private func fields(_ headers: HPACKHeaders) -> [HTTPHeaderField] {
        headers.compactMap { name, value, _ in
            name.hasPrefix(":") ? nil : HTTPHeaderField(name: name, value: value)
        }
    }

    private func emitRequestHead(_ headers: HPACKHeaders) {
        sink.receive(
            .requestHead(
                CapturedRequestHead(
                    id: requestID,
                    timestamp: Date(),
                    scheme: headers.first(name: ":scheme") ?? "https",
                    authority: headers.first(name: ":authority") ?? authority,
                    method: headers.first(name: ":method") ?? "",
                    path: headers.first(name: ":path") ?? "",
                    version: .http2,
                    headers: fields(headers),
                    target: target
                )
            )
        )
    }

    private func emitResponseHead(_ headers: HPACKHeaders, status: Int) {
        sink.receive(
            .responseHead(
                CapturedResponseHead(
                    requestID: requestID,
                    timestamp: Date(),
                    status: status,
                    version: .http2,
                    headers: fields(headers)
                )
            )
        )
    }

    private func emitTrailers(_ headers: HPACKHeaders) {
        let trailers = fields(headers)
        sink.receive(
            direction == .request
                ? .requestTrailers(requestID: requestID, headers: trailers)
                : .responseTrailers(requestID: requestID, headers: trailers)
        )
    }

    private func captureBody(_ data: IOData) {
        let fullSize = data.readableBytes
        let bytes: [UInt8]
        switch data {
        case .byteBuffer(let buffer):
            bytes = bodyBuffer.take(buffer.readableBytesView)
        case .fileRegion:
            bytes = bodyBuffer.take(EmptyCollection<UInt8>())
        }
        sink.receive(
            direction == .request
                ? .requestBodyChunk(requestID: requestID, bytes: bytes, byteCount: fullSize)
                : .responseBodyChunk(requestID: requestID, bytes: bytes, byteCount: fullSize)
        )
    }

    private func emitEnd() {
        guard phase != .ended, phase != .failed else { return }
        phase = .ended
        let truncated = bodyBuffer.truncated
        sink.receive(
            direction == .request
                ? .requestEnd(requestID: requestID, truncated: truncated)
                : .responseEnd(requestID: requestID, truncated: truncated)
        )
    }

    private func failCapture(terminateStream: Bool = false) {
        phase = .failed
        if terminateStream {
            streamContext?.value.terminate(failed: true)
        }
        if errorState.claim() {
            sink.receive(.streamError(requestID: requestID, message: Self.errorMessage))
        }
    }

    private var streamDirection: HTTP2StreamDirection {
        direction == .request ? .request : .response
    }
}

private extension HTTP2CaptureTapHandler {
    private func isResponseEndWindowUpdateError(_ error: Error) -> Bool {
        guard direction == .response,
              streamContext?.value.state == .acceptedWebSocket,
              let transition = error as? NIOHTTP2Errors.BadStreamStateTransition else {
            return false
        }
        return transition.fromState == .halfClosedRemoteLocalActive
    }

    private func isPostResponseEndCancellation(_ error: Error) -> Bool {
        guard direction == .response, phase == .ended,
              streamContext?.value.isWebSocketCaptureClosed == true,
              let streamClosed = error as? NIOHTTP2Errors.StreamClosed else {
            return false
        }
        return streamClosed.errorCode == .cancel
    }
}

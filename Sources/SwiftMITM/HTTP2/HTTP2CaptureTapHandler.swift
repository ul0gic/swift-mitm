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
    private let sink: CaptureEventSink
    private let errorState: HTTP2StreamErrorState
    private var bodyBuffer: CaptureBodyBuffer
    private var phase: Phase = .awaitingHead

    init(
        direction: Direction,
        requestID: UUID,
        authority: String,
        sink: CaptureEventSink,
        captureBodyLimit: Int = 0,
        errorState: HTTP2StreamErrorState = HTTP2StreamErrorState()
    ) {
        self.direction = direction
        self.requestID = requestID
        self.authority = authority
        self.sink = sink
        self.bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
        self.errorState = errorState
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let payload = unwrapInboundIn(data)
        switch payload {
        case .headers(let frame):
            processHeaders(frame.headers, endStream: frame.endStream)
        case .data(let frame):
            processData(frame.data, endStream: frame.endStream)
        case .rstStream:
            failCapture()
        default:
            break
        }
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        if phase != .ended, phase != .failed {
            failCapture()
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        failCapture()
        context.fireErrorCaught(error)
    }

    private func processHeaders(_ headers: HPACKHeaders, endStream: Bool) {
        switch (direction, phase) {
        case (.request, .awaitingHead):
            processInitialRequestHeaders(headers, endStream: endStream)
        case (.response, .awaitingHead):
            processInitialResponseHeaders(headers, endStream: endStream)
        case (_, .body):
            processTrailerHeaders(headers, endStream: endStream)
        case (_, .ended):
            failCapture()
        case (_, .failed):
            break
        }
    }

    private func processInitialRequestHeaders(_ headers: HPACKHeaders, endStream: Bool) {
        guard isValidRequestHead(headers) else { return failCapture() }
        emitRequestHead(headers)
        if endStream {
            emitEnd()
        } else {
            enterBody()
        }
    }

    private func processInitialResponseHeaders(_ headers: HPACKHeaders, endStream: Bool) {
        guard let status = validResponseStatus(headers) else { return failCapture() }
        guard status >= 200 else {
            guard !endStream else { return failCapture() }
            emitResponseHead(headers, status: status)
            return
        }
        emitResponseHead(headers, status: status)
        if endStream {
            emitEnd()
        } else {
            enterBody()
        }
    }

    private func processTrailerHeaders(_ headers: HPACKHeaders, endStream: Bool) {
        guard !containsPseudoHeaders(headers), endStream else { return failCapture() }
        emitTrailers(headers)
        emitEnd()
    }

    private func enterBody() {
        phase = .body
    }

    private func processData(_ data: IOData, endStream: Bool) {
        guard phase == .body else {
            if phase != .failed {
                failCapture()
            }
            return
        }
        captureBody(data)
        if endStream {
            emitEnd()
        }
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
                    headers: fields(headers)
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

    private func failCapture() {
        phase = .failed
        if errorState.claim() {
            sink.receive(.streamError(requestID: requestID, message: Self.errorMessage))
        }
    }
}

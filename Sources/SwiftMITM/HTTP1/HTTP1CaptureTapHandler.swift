import Foundation
import NIOConcurrencyHelpers
import NIOCore

final class WebSocketCloseEmissionState: Sendable {
    private let emitted = NIOLockedValueBox(false)

    func claim() -> Bool {
        emitted.withLockedValue { value in
            guard !value else { return false }
            value = true
            return true
        }
    }
}

final class HTTP1CaptureTapHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    enum Direction {
        case request
        case response
    }

    private let direction: Direction
    private let authority: String
    private let correlator: HTTP1ExchangeCorrelator
    private let sink: CaptureEventSink
    private let parser: HTTP1MessageParser
    private let captureBodyLimit: Int
    private let closeEmissionState: WebSocketCloseEmissionState
    private var currentID: UUID?
    private var bodyBuffer: CaptureBodyBuffer
    private var webSocket: WebSocketFrameDecoder?
    private var connectionID: UUID?
    private var permessageDeflate = false

    init(
        direction: Direction,
        authority: String,
        correlator: HTTP1ExchangeCorrelator,
        sink: CaptureEventSink,
        captureBodyLimit: Int = 0,
        closeEmissionState: WebSocketCloseEmissionState = WebSocketCloseEmissionState()
    ) {
        self.direction = direction
        self.authority = authority
        self.correlator = correlator
        self.sink = sink
        self.captureBodyLimit = captureBodyLimit
        self.closeEmissionState = closeEmissionState
        self.parser = HTTP1MessageParser(mode: direction == .request ? .request : .response)
        self.bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        parser.feed(
            buffer.readableBytesView,
            methodProvider: { [self] in
                let exchange = correlator.peek()
                currentID = exchange?.id
                return exchange?.method
            },
            consumeMethod: { [self] in _ = correlator.dequeue() },
            emit: { [self] output in handle(output) },
            bodyBytes: { [self] chunk in captureBody(chunk) },
            tunnelBytes: { [self] chunk in captureWebSocket(chunk) }
        )
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        parser.finish { [self] output in handle(output) }
        if webSocket != nil { emitWebSocketClose(code: nil, reason: nil) }
        context.fireChannelInactive()
    }

    private func handle(_ output: HTTP1ParserOutput) {
        switch output {
        case let .requestHead(method, path, headers):
            emitRequestHead(method: method, path: path, headers: headers)
        case let .responseHead(status, headers):
            emitResponseHead(status: status, headers: headers)
        case .bodyChunk:
            break
        case .trailers(let headers):
            guard let id = currentID else { return }
            sink.receive(
                direction == .request
                    ? .requestTrailers(requestID: id, headers: headers)
                    : .responseTrailers(requestID: id, headers: headers)
            )
        case .messageComplete:
            guard let id = currentID else { return }
            let truncated = bodyBuffer.truncated
            sink.receive(
                direction == .request
                    ? .requestEnd(requestID: id, truncated: truncated)
                    : .responseEnd(requestID: id, truncated: truncated)
            )
            currentID = nil
        case .upgraded:
            connectionID = currentID
            webSocket = WebSocketFrameDecoder(captureLimit: captureBodyLimit)
            if direction == .response, let id = connectionID {
                sink.receive(.webSocketOpen(connectionID: id, timestamp: Date(), permessageDeflate: permessageDeflate))
            }
        case .failed:
            break
        }
    }

    private func emitRequestHead(method: String, path: String, headers: [HTTPHeaderField]) {
        let id = UUID()
        currentID = id
        bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
        correlator.enqueue(id: id, method: method)
        let host = headers.first { $0.name.lowercased() == "host" }?.value ?? authority
        sink.receive(
            .requestHead(
                CapturedRequestHead(
                    id: id,
                    timestamp: Date(),
                    scheme: "https",
                    authority: host,
                    method: method,
                    path: path,
                    version: .http11,
                    headers: headers
                )
            )
        )
    }

    private func emitResponseHead(status: Int, headers: [HTTPHeaderField]) {
        let id = currentID ?? UUID()
        currentID = id
        bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
        permessageDeflate = Self.negotiatesPermessageDeflate(headers)
        sink.receive(
            .responseHead(
                CapturedResponseHead(
                    requestID: id,
                    timestamp: Date(),
                    status: status,
                    version: .http11,
                    headers: headers
                )
            )
        )
    }

    private func captureWebSocket<Bytes: Collection>(_ chunk: Bytes) where Bytes.Element == UInt8 {
        guard let id = connectionID, let decoder = webSocket else { return }
        let wsDirection: WebSocketDirection = direction == .request ? .clientToServer : .serverToClient
        decoder.decode(chunk) { [self] frame in
            sink.receive(
                .webSocketFrame(
                    CapturedWebSocketFrame(
                        connectionID: id,
                        timestamp: Date(),
                        direction: wsDirection,
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
            if frame.opcode == .connectionClose {
                emitWebSocketClose(code: frame.closeCode, reason: frame.closeReason)
            }
        }
    }

    private func emitWebSocketClose(code: Int?, reason: String?) {
        guard let id = connectionID, closeEmissionState.claim() else { return }
        sink.receive(.webSocketClose(connectionID: id, timestamp: Date(), code: code, reason: reason))
    }

    private static func negotiatesPermessageDeflate(_ headers: [HTTPHeaderField]) -> Bool {
        let field = headers.first { $0.name.lowercased() == "sec-websocket-extensions" }
        return field?.value.lowercased().contains("permessage-deflate") ?? false
    }

    private func captureBody<Bytes: Collection>(_ chunk: Bytes) where Bytes.Element == UInt8 {
        guard let id = currentID else { return }
        let fullSize = chunk.count
        let bytes = bodyBuffer.take(chunk)
        sink.receive(
            direction == .request
                ? .requestBodyChunk(requestID: id, bytes: bytes, byteCount: fullSize)
                : .responseBodyChunk(requestID: id, bytes: bytes, byteCount: fullSize)
        )
    }
}

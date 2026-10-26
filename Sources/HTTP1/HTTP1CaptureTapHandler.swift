import Foundation
import NIOCore

final class HTTP1CaptureTapHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    enum Direction {
        case request
        case response
    }

    private let direction: Direction
    private let authority: String
    private let scheme: String
    private let target: CapturedTarget?
    private let correlator: HTTP1ExchangeCorrelator
    private let sink: CaptureEventSink
    private let parser: HTTP1MessageParser
    private let captureBodyLimit: Int
    private let webSocketSession: WebSocketCaptureSession
    private var currentID: UUID?
    private var upgradeRequestID: UUID?
    private var responseUpgradeRequested = false
    private var bodyBuffer: CaptureBodyBuffer
    private var permessageDeflate = false

    init(
        direction: Direction,
        authority: String,
        scheme: String = "https",
        target: CapturedTarget? = nil,
        correlator: HTTP1ExchangeCorrelator,
        sink: CaptureEventSink,
        captureBodyLimit: Int = 0,
        webSocketSession: WebSocketCaptureSession = WebSocketCaptureSession()
    ) {
        self.direction = direction
        self.authority = authority
        self.scheme = scheme
        self.target = target
        self.correlator = correlator
        self.sink = sink
        self.captureBodyLimit = captureBodyLimit
        self.webSocketSession = webSocketSession
        self.parser = HTTP1MessageParser(mode: direction == .request ? .request : .response)
        self.bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        enterAcceptedWebSocketIfNeeded()
        parser.feed(
            buffer.readableBytesView,
            requestProvider: { [self] in
                let exchange = correlator.peek()
                currentID = exchange?.id
                responseUpgradeRequested = exchange?.webSocketUpgradeRequested ?? false
                return exchange.map {
                    HTTP1RequestMetadata(
                        method: $0.method,
                        webSocketUpgradeRequested: $0.webSocketUpgradeRequested
                    )
                }
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
        webSocketSession.close()
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
        case .upgradeRequested:
            upgradeRequestID = currentID
        case .upgraded:
            handleWebSocketUpgrade()
        case .failed:
            break
        }
    }

    private func handleWebSocketUpgrade() {
        guard let id = currentID else { return }
        webSocketSession.open(
            id: id,
            sink: sink,
            captureLimit: captureBodyLimit,
            permessageDeflate: permessageDeflate
        )
    }

    private func emitRequestHead(method: String, path: String, headers: [HTTPHeaderField]) {
        let id = UUID()
        currentID = id
        bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
        correlator.enqueue(
            id: id,
            method: method,
            webSocketUpgradeRequested: HTTP1MessageParser.isWebSocketUpgrade(headers)
        )
        let host = headers.first { $0.name.lowercased() == "host" }?.value ?? authority
        sink.receive(
            .requestHead(
                CapturedRequestHead(
                    id: id,
                    timestamp: Date(),
                    scheme: scheme,
                    authority: host,
                    method: method,
                    path: path,
                    version: .http11,
                    headers: headers,
                    target: target
                )
            )
        )
    }

    private func emitResponseHead(status: Int, headers: [HTTPHeaderField]) {
        let id = currentID ?? UUID()
        currentID = id
        bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
        permessageDeflate = Self.negotiatesPermessageDeflate(headers)
        resolveWebSocketUpgrade(status: status, headers: headers, id: id)
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

    private func resolveWebSocketUpgrade(status: Int, headers: [HTTPHeaderField], id: UUID) {
        guard responseUpgradeRequested, !((100..<200).contains(status) && status != 101) else { return }
        if status == 101, HTTP1MessageParser.isWebSocketUpgrade(headers) {
            correlator.acceptWebSocketUpgrade(id: id)
        } else {
            sink.receive(.requestEnd(requestID: id, truncated: false))
        }
        responseUpgradeRequested = false
    }

    private func enterAcceptedWebSocketIfNeeded() {
        guard direction == .request,
              let acceptedID = correlator.takeAcceptedWebSocketUpgradeID(),
              acceptedID == upgradeRequestID else { return }
        parser.phase = .tunnel
    }

    private func captureWebSocket<Bytes: Collection>(_ chunk: Bytes) where Bytes.Element == UInt8 {
        let wsDirection: WebSocketDirection = direction == .request ? .clientToServer : .serverToClient
        webSocketSession.capture(chunk, direction: wsDirection)
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

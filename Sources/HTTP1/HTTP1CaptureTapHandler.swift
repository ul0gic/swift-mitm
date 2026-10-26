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
    private let onResourceLimit: @Sendable () -> Void
    private var currentID: UUID?
    private var upgradeRequestID: UUID?
    private var responseUpgradeRequested = false
    private var bodyBuffer: CaptureBodyBuffer
    private var permessageDeflate = false
    private var resourceLimitReached = false

    init(
        direction: Direction,
        authority: String,
        scheme: String = "https",
        target: CapturedTarget? = nil,
        correlator: HTTP1ExchangeCorrelator,
        sink: CaptureEventSink,
        captureBodyLimit: Int = 0,
        webSocketSession: WebSocketCaptureSession = WebSocketCaptureSession(),
        onResourceLimit: @escaping @Sendable () -> Void = {}
    ) {
        self.direction = direction
        self.authority = authority
        self.scheme = scheme
        self.target = target
        self.correlator = correlator
        self.sink = sink
        self.captureBodyLimit = captureBodyLimit
        self.webSocketSession = webSocketSession
        self.onResourceLimit = onResourceLimit
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
            emit: { [self] output in handle(output) },
            bodyBytes: { [self] chunk in captureBody(chunk) },
            tunnelBytes: { [self] chunk in captureWebSocket(chunk) }
        )
        if resourceLimitReached {
            context.close(promise: nil)
            return
        }
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        parser.finish { [self] output in handle(output) }
        correlator.failOutstanding(sink: sink, message: "HTTP/1 transport closed before exchange completed")
        webSocketSession.close()
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let error = event as? ProxyConnectionError, error == .serverStopping {
            correlator.failOutstanding(sink: sink, message: "HTTP/1 proxy stopped before exchange completed")
            webSocketSession.close()
        }
        if let event = event as? ChannelEvent, case .inputClosed = event {
            parser.finish { [self] output in handle(output) }
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        correlator.failOutstanding(sink: sink, message: "HTTP/1 transport failed")
        webSocketSession.close()
        context.fireErrorCaught(error)
    }

    private func handle(_ output: HTTP1ParserOutput) {
        guard !correlator.isFailed else { return }
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
            correlator.end(id: id, request: direction == .request)
            currentID = nil
        case .upgradeRequested:
            upgradeRequestID = currentID
        case .upgraded:
            handleWebSocketUpgrade()
        case .failed:
            correlator.failOutstanding(sink: sink, message: "HTTP/1 capture parser failed")
        }
    }

    private func handleWebSocketUpgrade() {
        guard let id = currentID else { return }
        sink.receive(.responseEnd(requestID: id, truncated: false))
        correlator.end(id: id, request: false)
        currentID = nil
        webSocketSession.open(
            id: id,
            sink: sink,
            captureLimit: captureBodyLimit,
            permessageDeflate: permessageDeflate
        )
    }

    private func emitRequestHead(method: String, path: String, headers: [HTTPHeaderField]) {
        let id = UUID()
        guard correlator.enqueue(
            id: id,
            method: method,
            webSocketUpgradeRequested: HTTP1MessageParser.isWebSocketUpgrade(headers),
            requestEnded: false
        ) else {
            resourceLimitReached = true
            correlator.failOutstanding(sink: sink, message: "HTTP/1 correlation resource limit exceeded")
            onResourceLimit()
            return
        }
        currentID = id
        bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
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
        guard let id = currentID else { return }
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
        }
        sink.receive(.requestEnd(requestID: id, truncated: false))
        correlator.end(id: id, request: true)
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
        guard !correlator.isFailed, let id = currentID else { return }
        let fullSize = chunk.count
        (sink as? ObservationCaptureSink)?.receiveBody(
            requestID: id,
            direction: direction == .request ? .request : .response,
            bytes: chunk
        )
        let bytes = bodyBuffer.take(chunk)
        sink.receive(
            direction == .request
                ? .requestBodyChunk(requestID: id, bytes: bytes, byteCount: fullSize)
                : .responseBodyChunk(requestID: id, bytes: bytes, byteCount: fullSize)
        )
    }
}

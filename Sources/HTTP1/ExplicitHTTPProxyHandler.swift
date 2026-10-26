import Foundation
import NIOCore
import NIOHTTP1
import SwiftMITMHTTP1Codec

final class ExplicitHTTPProxyHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    final class Exchange {
        let id: UUID
        let head: HTTPRequestHead
        let target: HTTPForwardTarget?
        let requestKeepAlive: Bool
        let timestamp = Date()
        var pending: [HTTPServerRequestPart] = []
        var requestBody: CaptureBodyBuffer
        var responseBody: CaptureBodyBuffer
        var requestEnded = false
        var responseStarted = false
        var responseEnded = false
        var earlyResponse = false
        var upgrading = false
        var permessageDeflate = false
        var published = false
        var originKeepAlive: Bool?
        var responseConnectionTokens: Set<String> = []

        init(id: UUID, head: HTTPRequestHead, target: HTTPForwardTarget?, bodyLimit: Int, requestKeepAlive: Bool) {
            self.id = id
            self.head = head
            self.target = target
            self.requestKeepAlive = requestKeepAlive
            requestBody = CaptureBodyBuffer(limit: bodyLimit)
            responseBody = CaptureBodyBuffer(limit: bodyLimit)
        }
    }

    let sink: CaptureEventSink
    let captureBodyLimit: Int
    let correlator: HTTP1ExchangeCorrelator
    let connectionID: UUID
    let connect: @Sendable (HTTPForwardTarget, EventLoop) -> EventLoopFuture<HTTPForwardConnection>
    let onCONNECT: @Sendable (Channel, String) -> Void
    let onResourceLimit: @Sendable () -> Void
    var queue: [Exchange?]
    var first = 0
    var count = 0
    var receiving: Exchange?
    var active: Exchange?
    var upstream: Channel?
    var context: ChannelHandlerContext?
    var stopped = false
    var connectionFailureEmitted = false
    var inputClosed = false
    var pendingRequestKeepAlive: Bool?
    var pendingResponseKeepAlive: Bool?

    init(
        sink: CaptureEventSink,
        captureBodyLimit: Int,
        correlationPolicy: HTTP1CorrelationPolicy = .default,
        connectionID: UUID,
        connect: @escaping @Sendable (HTTPForwardTarget, EventLoop) -> EventLoopFuture<HTTPForwardConnection>,
        onCONNECT: @escaping @Sendable (Channel, String) -> Void,
        onResourceLimit: @escaping @Sendable () -> Void = {}
    ) {
        self.sink = sink
        self.captureBodyLimit = captureBodyLimit
        self.connectionID = connectionID
        self.connect = connect
        self.onCONNECT = onCONNECT
        self.onResourceLimit = onResourceLimit
        correlator = HTTP1ExchangeCorrelator(policy: correlationPolicy)
        queue = Array(repeating: nil, count: correlationPolicy.maximumPendingExchanges)
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        context.channel.setOption(ChannelOptions.autoRead, value: false).assumeIsolated().whenComplete { result in
            switch result {
            case .success:
                context.read()
            case .failure:
                self.abort(message: "HTTP/1 request admission failed")
            }
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.context = nil
    }

    func channelActive(context: ChannelHandlerContext) {
        context.read()
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !stopped else { return }
        let part = unwrapInboundIn(data)
        switch part {
        case .head(let head):
            guard let isKeepAlive = pendingRequestKeepAlive else {
                reject(status: .badRequest, message: "Missing HTTP/1 request parser metadata")
                return
            }
            pendingRequestKeepAlive = nil
            admit(head, isKeepAlive: isKeepAlive)
        case .body:
            guard let exchange = receiving, exchange.head.method != .CONNECT else {
                reject(status: .badRequest, message: "Invalid HTTP/1 request body")
                return
            }
            captureRequest(part, exchange: exchange)
            forward(part, for: exchange)
        case .end:
            guard let exchange = receiving else {
                reject(status: .badRequest, message: "Unexpected HTTP/1 request end")
                return
            }
            exchange.requestEnded = true
            captureRequest(part, exchange: exchange)
            correlator.end(id: exchange.id, request: true)
            receiving = nil
            if exchange.head.method == .CONNECT {
                if active === exchange { establishCONNECT(exchange) }
            } else {
                forward(part, for: exchange)
            }
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        upstream?.flush()
        readClientIfNeeded()
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        if context.channel.isWritable { upstream?.read() }
        context.fireChannelWritabilityChanged()
    }

    func channelInactive(context: ChannelHandlerContext) {
        abort(message: "HTTP/1 client closed before exchange completed")
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let error = event as? ProxyConnectionError, error == .serverStopping {
            cancel()
            return
        }
        if let metadata = event as? BytePreservingHTTP1MessageMetadata {
            pendingRequestKeepAlive = metadata.isKeepAlive
            return
        }
        if let event = event as? ChannelEvent, case .inputClosed = event {
            inputClosed = true
            if receiving != nil {
                abort(message: "HTTP/1 request ended prematurely")
            } else if active == nil {
                context.close(promise: nil)
            }
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        reject(status: .badRequest, message: "Invalid HTTP/1 request framing")
    }

    func admit(_ head: HTTPRequestHead, isKeepAlive: Bool) {
        guard receiving == nil, !Self.ambiguous(head.headers), head.version == .http1_1 else {
            reject(status: .badRequest, message: "Invalid HTTP/1 request framing")
            return
        }
        let target = HTTPForwardTarget(head: head)
        if head.method == .CONNECT {
            guard ConnectionTarget(explicitConnectAuthority: head.uri) != nil,
                !head.headers.contains(name: "transfer-encoding"),
                head.headers["content-length"].allSatisfy({ $0 == "0" }) else {
                reject(status: .badRequest, message: "Invalid CONNECT request")
                return
            }
        } else if target == nil {
            reject(status: .badRequest, message: "Invalid absolute HTTP request target")
            return
        }
        let id = UUID()
        guard correlator.enqueue(id: id, method: head.method.rawValue, requestEnded: false, published: false) else {
            connectionFailureEmitted = true
            abort(message: "HTTP/1 correlation resource limit exceeded", close: false)
            onResourceLimit()
            context?.close(promise: nil)
            return
        }
        let exchange = Exchange(
            id: id,
            head: head,
            target: target,
            bodyLimit: captureBodyLimit,
            requestKeepAlive: isKeepAlive
        )
        queue[(first + count) % queue.count] = exchange
        count += 1
        receiving = exchange
        if active == nil { startNext() }
    }

    func startNext() {
        guard !stopped, active == nil, let exchange = queue[first], let context else { return }
        active = exchange
        guard let target = exchange.target else {
            if exchange.requestEnded { establishCONNECT(exchange) }
            return
        }
        connect(target, context.eventLoop).hop(to: context.eventLoop).assumeIsolated().whenComplete { result in
            switch result {
            case .failure(let error):
                guard !self.stopped else { return }
                self.diagnoseConnect(error)
                self.reject(status: .badGateway, message: "HTTP/1 origin connection failed")
            case .success(let connection):
                guard !self.stopped, self.active === exchange else {
                    connection.channel.close(promise: nil)
                    return
                }
                self.configureOrigin(connection, exchange: exchange)
            }
        }
    }

    func configureOrigin(_ connection: HTTPForwardConnection, exchange: Exchange) {
        guard let target = exchange.target else { return }
        let channel = connection.channel
        upstream = channel
        do {
            var configuration = BytePreservingHTTPRequestEncoder.Configuration()
            configuration.automaticallySetFramingHeaders = false
            try channel.pipeline.syncOperations.addHandlers([
                BytePreservingHTTPRequestEncoder(configuration: configuration),
                ByteToMessageHandler(BytePreservingHTTPResponseDecoder(
                    leftOverBytesStrategy: .forwardBytes,
                    informationalResponseStrategy: .forward
                )),
                HTTPForwardResponseHandler(owner: self)
            ])
            captureRequestHead(exchange, target: connection.target)
            var head = exchange.head
            head.uri = target.requestTarget
            head.headers = Self.hopHeaders(head.headers, upgrading: Self.isWebSocket(head.headers))
            head.headers.replaceOrAdd(name: "host", value: target.authority)
            Self.frameRequest(&head, original: exchange.head.headers)
            channel.write(NIOAny(HTTPClientRequestPart.head(head)), promise: nil)
            let pending = exchange.pending
            exchange.pending.removeAll()
            for part in pending {
                captureRequest(part, exchange: exchange)
                write(part, exchange: exchange, to: channel)
            }
            channel.flush()
            channel.read()
            readClientIfNeeded()
        } catch {
            reject(status: .badGateway, message: "HTTP/1 origin pipeline failed")
        }
    }

    func forward(_ part: HTTPServerRequestPart, for exchange: Exchange) {
        if active === exchange, let upstream, !exchange.earlyResponse {
            write(part, exchange: exchange, to: upstream)
        } else {
            exchange.pending.append(part)
        }
    }

    func write(_ part: HTTPServerRequestPart, exchange: Exchange, to channel: Channel) {
        switch part {
        case .head:
            break
        case .body(let buffer):
            channel.writeAndFlush(NIOAny(HTTPClientRequestPart.body(.byteBuffer(buffer))), promise: nil)
        case .end(let trailers):
            channel.writeAndFlush(NIOAny(HTTPClientRequestPart.end(trailers.map {
                Self.hopHeaders(
                    $0,
                    upgrading: false,
                    appendVia: false,
                    excluding: HTTP1MessageParser.tokens(Self.fields(exchange.head.headers), named: "connection")
                )
            })), promise: nil)
        }
    }
}

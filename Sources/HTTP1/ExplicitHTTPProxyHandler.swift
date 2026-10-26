import Foundation
import NIOCore
import NIOHTTP1
import SwiftMITMHTTP1Codec

final class ExplicitHTTPProxyHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    let sink: CaptureEventSink
    let captureBodyLimit: Int
    let correlator: HTTP1ExchangeCorrelator
    let connectionID: UUID
    let connect: @Sendable (HTTPForwardTarget, EventLoop) -> EventLoopFuture<HTTPForwardConnection>
    let onCONNECT: @Sendable (Channel, String) -> Void
    let onResourceLimit: @Sendable () -> Void
    let controlRuntime: HTTPControlRuntime?
    let controlEventSink: any HTTPControlEventSink?
    let admission: ResourceAdmissionController?
    let fixedDestination: HTTPForwardTarget?
    let typedHTTP2: Bool
    let externallyCaptured: Bool
    let suppliedRequestID: UUID?
    let reusePool: HTTP1ReusePool?
    let authorizeReuse: @Sendable (HTTPForwardTarget, Channel) -> Bool
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
        connectionID: UUID,
        connect: @escaping @Sendable (HTTPForwardTarget, EventLoop) -> EventLoopFuture<HTTPForwardConnection>,
        onCONNECT: @escaping @Sendable (Channel, String) -> Void,
        options: HTTPForwardOptions = HTTPForwardOptions()
    ) {
        self.sink = sink
        self.captureBodyLimit = captureBodyLimit
        self.connectionID = connectionID
        self.connect = connect
        self.onCONNECT = onCONNECT
        onResourceLimit = options.onResourceLimit
        controlRuntime = options.controlRuntime
        controlEventSink = options.controlEventSink
        admission = options.admission
        fixedDestination = options.fixedDestination
        typedHTTP2 = options.typedHTTP2
        externallyCaptured = options.externallyCaptured
        suppliedRequestID = options.requestID
        reusePool = options.reusePool
        authorizeReuse = options.authorizeReuse
        correlator = HTTP1ExchangeCorrelator(policy: options.correlationPolicy)
        queue = Array(repeating: nil, count: options.correlationPolicy.maximumPendingExchanges)
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        let channel = context.channel
        var ready = channel.setOption(ChannelOptions.autoRead, value: false)
        if controlRuntime != nil, !typedHTTP2 {
            ready = ready.flatMap { channel.setOption(ChannelOptions.maxMessagesPerRead, value: 1) }
        }
        ready.assumeIsolated().whenComplete { result in
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
            guard let isKeepAlive = typedHTTP2 ? true : pendingRequestKeepAlive else {
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
            if retainControlledRequest(part, exchange: exchange) {
                return
            }
            forward(part, for: exchange)
        case .end(let trailers):
            receiveRequestEnd(trailers)
        }
    }

    func receiveRequestEnd(_ trailers: HTTPHeaders?) {
        guard let exchange = receiving else {
            reject(status: .badRequest, message: "Unexpected HTTP/1 request end")
            return
        }
        exchange.requestEnded = true
        exchange.requestControlTrailers = trailers
        captureRequest(.end(trailers), exchange: exchange)
        correlator.end(id: exchange.id, request: true)
        receiving = nil
        if exchange.controlCollectingBody {
            guard trailers?.isEmpty ?? true else {
                controlFailed(exchange, error: HTTPControlError.unsupportedEdit)
                return
            }
            completeControlledRequest(exchange)
            return
        }
        if !exchange.controlReady {
            return
        }
        if exchange.head.method == .CONNECT {
            if active === exchange {
                establishCONNECT(exchange)
            }
        } else {
            forward(.end(trailers), for: exchange)
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        upstream?.flush()
        readClientIfNeeded()
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        active?.responseFaultWriter?.drain()
        readOriginIfNeeded()
        context.fireChannelWritabilityChanged()
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.abort(message: "HTTP/1 client closed before exchange completed")
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
        let target = forwardTarget(for: head)
        guard validateAdmission(head, target: target) else { return }
        let id = suppliedRequestID ?? UUID()
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
        do {
            if !typedHTTP2 {
                exchange.admissionReservation = try admission?.acquire(.stream)
            }
        } catch {
            reject(status: .serviceUnavailable, message: "HTTP stream admission exceeded")
            return
        }
        if !prepareControlledRequest(exchange) {
            return
        }
        if active == nil {
            startNext()
        }
    }

    func validateAdmission(_ head: HTTPRequestHead, target: HTTPForwardTarget?) -> Bool {
        guard receiving == nil, !Self.ambiguous(head.headers),
              head.version == .http1_1 || (typedHTTP2 && head.version.major == 2) else {
            reject(status: .badRequest, message: "Invalid HTTP/1 request framing")
            return false
        }
        if head.method == .CONNECT {
            guard ConnectionTarget(explicitConnectAuthority: head.uri) != nil,
                !head.headers.contains(name: "transfer-encoding"),
                head.headers["content-length"].allSatisfy({ $0 == "0" }) else {
                reject(status: .badRequest, message: "Invalid CONNECT request")
                return false
            }
        } else if target == nil {
            reject(status: .badRequest, message: "Invalid absolute HTTP request target")
            return false
        }
        return true
    }

    func startNext() {
        guard !stopped, active == nil, let exchange = queue[first], exchange.controlReady, let context else { return }
        active = exchange
        if let decision = exchange.syntheticDecision, case .respond(let status, _, let body) = decision.action {
            guard let headers = decision.syntheticResponseHeaders else {
                controlFailed(exchange, error: HTTPControlError.invalidHeaders)
                return
            }
            sendControlledResponse(status: status, headers: headers, body: body, exchange: exchange)
            return
        }
        guard let target = exchange.target else {
            if exchange.requestEnded {
                establishCONNECT(exchange)
            }
            return
        }
        if takeReusableOrigin(exchange, target: target) {
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
}

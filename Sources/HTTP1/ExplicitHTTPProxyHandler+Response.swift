import Foundation
import NIOCore
import NIOHTTP1

extension ExplicitHTTPProxyHandler {
    func receive(_ part: HTTPClientResponsePart) {
        guard !stopped, let exchange = active, let context else { return }
        switch part {
        case .head(let head):
            guard let isKeepAlive = pendingResponseKeepAlive else {
                originFailed()
                return
            }
            pendingResponseKeepAlive = nil
            receiveHead(head, exchange: exchange, context: context, isKeepAlive: isKeepAlive)
        case .body(let body):
            sink.receive(.responseBodyChunk(
                requestID: exchange.id,
                bytes: exchange.responseBody.take(body.readableBytesView),
                byteCount: body.readableBytes
            ))
            context.writeAndFlush(wrapOutboundOut(.body(.byteBuffer(body))), promise: nil)
        case .end(let trailers):
            receiveEnd(trailers, exchange: exchange, context: context)
        }
    }

    func receiveEnd(_ trailers: HTTPHeaders?, exchange: Exchange, context: ChannelHandlerContext) {
        exchange.responseEnded = true
        if let trailers, !trailers.isEmpty {
            sink.receive(.responseTrailers(requestID: exchange.id, headers: Self.fields(trailers)))
        }
        sink.receive(.responseEnd(requestID: exchange.id, truncated: exchange.responseBody.truncated))
        correlator.end(id: exchange.id, request: false)
        if exchange.originKeepAlive == false, !exchange.upgrading {
            let origin = upstream
            upstream = nil
            origin?.close(promise: nil)
        }
        let end = HTTPServerResponsePart.end(trailers.map {
            Self.hopHeaders($0, upgrading: false, appendVia: false, excluding: exchange.responseConnectionTokens)
        })
        context.writeAndFlush(wrapOutboundOut(end)).assumeIsolated().whenComplete { result in
            switch result {
            case .success:
                if exchange.upgrading {
                    self.upgradeWebSocket(exchange)
                } else {
                    self.finish(exchange)
                }
            case .failure:
                self.abort(message: "HTTP/1 response write failed")
            }
        }
    }

    func receiveHead(
        _ receivedHead: HTTPResponseHead,
        exchange: Exchange,
        context: ChannelHandlerContext,
        isKeepAlive: Bool
    ) {
        guard !Self.ambiguous(receivedHead.headers) else {
            originFailed()
            return
        }
        let informational = (100..<200).contains(receivedHead.status.code) && receivedHead.status.code != 101
        let upgrade = receivedHead.status.code == 101
        guard !upgrade || (Self.isWebSocket(exchange.head.headers) && Self.isWebSocket(receivedHead.headers)) else {
            originFailed()
            return
        }
        if !informational {
            exchange.responseStarted = true
            exchange.originKeepAlive = isKeepAlive
            exchange.responseConnectionTokens = HTTP1MessageParser.tokens(
                Self.fields(receivedHead.headers), named: "connection"
            )
            exchange.earlyResponse = !exchange.requestEnded
            exchange.upgrading = upgrade
            exchange.permessageDeflate = receivedHead.headers["sec-websocket-extensions"].contains {
                $0.lowercased().contains("permessage-deflate")
            }
        }
        sink.receive(.responseHead(CapturedResponseHead(
            requestID: exchange.id,
            timestamp: Date(),
            status: Int(receivedHead.status.code),
            version: .http11,
            headers: Self.fields(receivedHead.headers)
        )))
        forwardResponseHead(receivedHead, exchange: exchange, context: context)
    }

    func forwardResponseHead(_ receivedHead: HTTPResponseHead, exchange: Exchange, context: ChannelHandlerContext) {
        var head = receivedHead
        head.version = .http1_1
        head.headers = Self.hopHeaders(receivedHead.headers, upgrading: exchange.upgrading)
        let hasBody = exchange.head.method != .HEAD && head.status.mayHaveResponseBody && !exchange.upgrading
        if hasBody {
            Self.frameResponse(&head.headers, original: receivedHead.headers)
        } else {
            head.headers.remove(name: "transfer-encoding")
            if exchange.head.method != .HEAD, head.status != .notModified {
                head.headers.remove(name: "content-length")
            }
        }
        if !exchange.requestKeepAlive || exchange.earlyResponse {
            head.headers.replaceOrAdd(name: "connection", value: "close")
        }
        context.writeAndFlush(wrapOutboundOut(.head(head)), promise: nil)
    }

    func finish(_ exchange: Exchange) {
        guard !stopped, active === exchange else { return }
        let origin = upstream
        upstream = nil
        origin?.close(promise: nil)
        if exchange.earlyResponse || !exchange.requestKeepAlive {
            abort(message: "HTTP/1 connection ended with unfinished requests")
            return
        }
        queue[first] = nil
        first = (first + 1) % queue.count
        count -= 1
        active = nil
        if count > 0 {
            startNext()
        } else if inputClosed {
            context?.close(promise: nil)
        } else {
            context?.read()
        }
    }

    func readClientIfNeeded() {
        guard !stopped, let exchange = active, !exchange.requestEnded, !exchange.earlyResponse,
            upstream?.isWritable == true else { return }
        context?.read()
    }

    func readOriginIfNeeded() {
        guard !stopped, context?.channel.isWritable == true else { return }
        upstream?.read()
    }

    func originFailed(channel: Channel? = nil) {
        guard !stopped, let upstream, active?.responseEnded != true else { return }
        if let channel, upstream !== channel { return }
        if active?.responseStarted == true {
            abort(message: "Invalid or incomplete HTTP/1 origin response")
        } else {
            reject(status: .badGateway, message: "Invalid HTTP/1 origin response framing")
        }
    }

    func reject(status: HTTPResponseStatus, message: String) {
        guard !stopped, let context else { return }
        let started = active?.responseStarted == true
        if active?.published != true {
            diagnose(reason: .transportFailure, stage: .admission, code: .transportFailure)
        }
        abort(message: message, close: false)
        if started {
            context.close(promise: nil)
            return
        }
        let head = HTTPResponseHead(version: .http1_1, status: status, headers: [
            "content-length": "0", "connection": "close", "via": "1.1 SwiftMITM"
        ])
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil))).assumeIsolated().whenComplete { _ in
            context.close(promise: nil)
        }
    }

    func abort(message: String, close: Bool = true) {
        guard !stopped else { return }
        if queue.contains(where: { $0?.published == false }) {
            diagnose(reason: .transportFailure, stage: .capture, code: .transportFailure)
        }
        stopped = true
        correlator.failOutstanding(sink: sink, message: message)
        let origin = upstream
        upstream = nil
        origin?.close(promise: nil)
        queue = Array(repeating: nil, count: queue.count)
        receiving = nil
        active = nil
        count = 0
        if close { context?.close(promise: nil) }
    }

    func cancel() {
        guard !stopped else { return }
        if queue.contains(where: { $0?.published == false }) {
            diagnose(reason: .cancelled, stage: .capture, code: .serverStopping)
        }
        abort(message: "HTTP/1 proxy stopped before exchange completed", close: false)
    }
}

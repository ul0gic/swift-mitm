import Foundation
import NIOCore
import NIOHTTP1

extension ExplicitHTTPProxyHandler {
    func receive(_ part: HTTPClientResponsePart) {
        guard !stopped, let exchange = active, let context else { return }
        guard !exchange.responseEnded else {
            exchange.reusePoisoned = true
            upstream?.close(promise: nil)
            return
        }
        switch part {
        case .head(let head):
            guard let isKeepAlive = exchange.upstreamHTTP2 ? true : pendingResponseKeepAlive else {
                originFailed()
                return
            }
            pendingResponseKeepAlive = nil
            receiveHead(head, exchange: exchange, context: context, isKeepAlive: isKeepAlive)
        case .body(let body):
            if !externallyCaptured {
            (sink as? ObservationCaptureSink)?.receiveBody(
                requestID: exchange.id, direction: .response, bytes: body.readableBytesView
            )
            sink.receive(.responseBodyChunk(
                requestID: exchange.id,
                bytes: exchange.responseBody.take(body.readableBytesView),
                byteCount: body.readableBytes
            ))
            }
            if retainControlledResponse(body, exchange: exchange) { return }
            if let writer = exchange.responseFaultWriter {
                do { try writer.enqueue(body) }
                catch { controlFailed(exchange, error: error) }
            } else { writeResponseBody(body, exchange: exchange, context: context) }
        case .end(let trailers):
            if exchange.responseControlHead != nil {
                completeControlledResponse(trailers, exchange: exchange)
                return
            }
            receiveEnd(trailers, exchange: exchange, context: context)
        }
    }

    func receiveEnd(_ trailers: HTTPHeaders?, exchange: Exchange, context: ChannelHandlerContext) {
        exchange.responseEnded = true
        if !externallyCaptured, let trailers, !trailers.isEmpty {
            sink.receive(.responseTrailers(requestID: exchange.id, headers: Self.fields(trailers)))
        }
        if !externallyCaptured { sink.receive(.responseEnd(requestID: exchange.id, truncated: exchange.responseBody.truncated)) }
        correlator.end(id: exchange.id, request: false)
        if exchange.originKeepAlive == false, !exchange.upgrading {
            let origin = upstream
            upstream = nil
            origin?.close(promise: nil)
        }
        let end = HTTPServerResponsePart.end(trailers.map {
            Self.hopHeaders($0, upgrading: false, appendVia: false, excluding: exchange.responseConnectionTokens)
        })
        let complete = {
            if exchange.controlSnapshot != nil {
                if case .end(let fields) = end, let fields, !fields.isEmpty {
                    (self.sink as? ObservationCaptureSink)?.receiveEffective(.responseTrailers(requestID: exchange.id, headers: Self.fields(fields)))
                }
                (self.sink as? ObservationCaptureSink)?.receiveEffective(.responseEnd(requestID: exchange.id, truncated: false))
            }
            context.writeAndFlush(self.wrapOutboundOut(end)).assumeIsolated().whenComplete { result in
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
        if let writer = exchange.responseFaultWriter { writer.finish(complete) }
        else { complete() }
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
            exchange.reusableFraming = exchange.head.method == .HEAD || !receivedHead.status.mayHaveResponseBody
                || receivedHead.headers.contains(name: "content-length")
                || HTTP1MessageParser.tokens(Self.fields(receivedHead.headers), named: "transfer-encoding").contains("chunked")
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
        if !externallyCaptured { sink.receive(.responseHead(CapturedResponseHead(
            requestID: exchange.id,
            timestamp: Date(),
            status: Int(receivedHead.status.code),
            version: .http11,
            headers: Self.fields(receivedHead.headers)
        ))) }
        if prepareControlledResponse(receivedHead, exchange: exchange) { return }
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
        if typedHTTP2 {
            head.headers.remove(name: "connection")
            head.headers.remove(name: "transfer-encoding")
            head.headers = HTTPHeaders(head.headers.map { ($0.name.lowercased(), $0.value) })
        }
        if exchange.controlSnapshot != nil {
            controlEventSink?.receive(.effectiveHead(
                requestID: exchange.id, phase: .response,
                headers: Self.fields(head.headers), appliedRuleIDs: exchange.responseControlRuleIDs
            ))
            (sink as? ObservationCaptureSink)?.receiveEffective(.responseHead(CapturedResponseHead(
                requestID: exchange.id, timestamp: Date(), status: Int(head.status.code), version: typedHTTP2 ? .http2 : .http11,
                headers: Self.fields(head.headers)
            )))
        }
        context.writeAndFlush(wrapOutboundOut(.head(head)), promise: nil)
    }

    func finish(_ exchange: Exchange) {
        guard !stopped, active === exchange else { return }
        if typedHTTP2, !exchange.requestEnded {
            readClientIfNeeded()
            return
        }
        if finishWithReuse(exchange) { return }
        finishExchange(exchange)
    }

    func finishExchange(_ exchange: Exchange) {
        guard !stopped, active === exchange else { return }
        let origin = upstream
        if let origin { retainControlBytesUntilClosed(origin, exchange: exchange) }
        upstream = nil
        origin?.close(promise: nil)
        if (exchange.earlyResponse && !typedHTTP2) || !exchange.requestKeepAlive {
            abort(message: "HTTP/1 connection ended with unfinished requests")
            return
        }
        queue[first] = nil
        exchange.controlReservation = nil
        exchange.responseControlReservation = nil
        exchange.controlAdmissionReservation = nil
        exchange.responseControlAdmissionReservation = nil
        exchange.breakpointEditReservation = nil
        exchange.breakpointEditAdmissionReservation = nil
        exchange.requestFaultWriter?.cancel()
        exchange.responseFaultWriter?.cancel()
        exchange.admissionReservation?.release()
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
        if active?.requestFaultWriter?.hasPending == true { return }
        if let receiving, receiving.controlCollectingBody, !stopped {
            context?.read()
            return
        }
        guard !stopped, let exchange = active, !exchange.requestEnded, !exchange.earlyResponse || typedHTTP2,
            upstream?.isWritable == true else { return }
        context?.read()
    }

    func readOriginIfNeeded() {
        if active?.responseFaultWriter?.hasPending == true { return }
        if let active, active.responseControlHead != nil, !active.responseControlCollecting { return }
        guard !stopped, context?.channel.isWritable == true else { return }
        upstream?.read()
    }

    func originFailed(channel: Channel? = nil) {
        if typedHTTP2, active?.responseEnded == true, active?.requestEnded == false {
            abort(message: "HTTP/2 origin closed before request completion")
            return
        }
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
        reusePool?.close(downstreamID: connectionID)
        for exchange in queue.compactMap({ $0 }) {
            if let context { retainControlBytesUntilClosed(context.channel, exchange: exchange) }
            if active === exchange, let upstream { retainControlBytesUntilClosed(upstream, exchange: exchange) }
            exchange.controlTask?.cancel()
            exchange.controlReservation = nil
            exchange.responseControlReservation = nil
            exchange.controlAdmissionReservation = nil
            exchange.responseControlAdmissionReservation = nil
            exchange.breakpointEditReservation = nil
            exchange.breakpointEditAdmissionReservation = nil
            exchange.requestFaultWriter?.cancel()
            exchange.responseFaultWriter?.cancel()
            exchange.admissionReservation?.release()
            if let runtime = controlRuntime {
                let requestID = exchange.id
                Task { await runtime.cancel(requestID: requestID) }
            }
        }
        if !externallyCaptured { correlator.failOutstanding(sink: sink, message: message) }
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

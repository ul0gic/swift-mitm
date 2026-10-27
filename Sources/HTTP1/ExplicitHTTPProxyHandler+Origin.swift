import Foundation
import NIOCore
import NIOHTTP1
import NIOHTTP2
import SwiftMITMHTTP1Codec

extension ExplicitHTTPProxyHandler {
    func configureOrigin(_ connection: HTTPForwardConnection, exchange: Exchange) {
        guard let target = exchange.target else { return }
        let channel = connection.channel
        exchange.upstreamHTTP2 = connection.usesHTTP2Codec
        upstream = channel
        if exchange.controlSnapshot != nil {
            controlEventSink?.receive(.connectedTarget(requestID: exchange.id, target: connection.target))
        }
        do {
            try installOriginCodecs(connection, exchange: exchange, target: target)
            if !exchange.published {
                captureRequestHead(exchange, target: connection.target)
            }
            let head = originHead(exchange, target: target)
            if let fault = exchange.requestFault {
                exchange.requestFaultWriter = HTTPControlFaultWriter(
                    fault: fault,
                    channel: channel,
                    emit: { [weak self, weak exchange] bytes in
                        guard let self, let exchange else { return }
                        self.writeRequestBody(bytes, exchange: exchange, to: channel)
                    },
                    onDrain: { [weak self] in self?.readClientIfNeeded() },
                    onReset: { [weak self, weak exchange] scope in
                        guard let self, let exchange else { return }
                        self.resetControlFault(scope, direction: .request, exchange: exchange)
                    }
                )
                exchange.requestFaultWriter?.beginHead {
                    self.writeOriginHead(head, exchange: exchange, channel: channel)
                }
            } else {
                writeOriginHead(head, exchange: exchange, channel: channel)
            }
            if let release = connection.releaseBufferedReads {
                let owner = NIOLoopBound(self, eventLoop: channel.eventLoop)
                let restored = release().flatMap { channel.setOption(ChannelOptions.autoRead, value: false) }
                restored.whenSuccess { _ in
                    guard owner.value.upstream === channel else { return }
                    // Disabling autoRead cancels the pending socket read established before ALPN buffer release.
                    owner.value.readOriginIfNeeded()
                }
                restored.whenFailure { _ in channel.close(promise: nil) }
            }
            readClientIfNeeded()
        } catch {
            reject(status: .badGateway, message: "HTTP/1 origin pipeline failed")
        }
    }

    func installOriginCodecs(
        _ connection: HTTPForwardConnection,
        exchange: Exchange,
        target: HTTPForwardTarget
    ) throws {
        let channel = connection.channel
        if connection.usesHTTP2Codec {
            try channel.pipeline.syncOperations.addHandlers([
                HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: target.scheme == "https" ? .https : .http),
                HTTPForwardResponseHandler(owner: self)
            ])
        } else {
            var configuration = BytePreservingHTTPRequestEncoder.Configuration()
            configuration.automaticallySetFramingHeaders = false
            let decoder = BytePreservingHTTPResponseDecoder(
                leftOverBytesStrategy: .forwardBytes,
                informationalResponseStrategy: .forward
            )
            exchange.responseDecoder = decoder
            try channel.pipeline.syncOperations.addHandlers([
                BytePreservingHTTPRequestEncoder(configuration: configuration),
                ByteToMessageHandler(decoder),
                HTTPForwardResponseHandler(owner: self)
            ])
        }
    }

    func originHead(_ exchange: Exchange, target: HTTPForwardTarget) -> HTTPRequestHead {
        var head = exchange.effectiveHead
        head.uri = target.requestTarget
        head.headers = Self.hopHeaders(head.headers, upgrading: Self.isWebSocket(head.headers))
        if exchange.upstreamHTTP2,
           !HTTP1MessageParser.tokens(Self.fields(exchange.effectiveHead.headers), named: "connection").contains("te") {
            // RFC 9113 §8.2.2 permits TE: trailers; gRPC requires that capability on its HTTP/2 hop.
            for value in exchange.effectiveHead.headers["te"]
                where value.lowercased().trimmingCharacters(in: .whitespaces) == "trailers" {
                head.headers.add(name: "te", value: value)
            }
        }
        head.headers.replaceOrAdd(name: "host", value: target.authority)
        Self.frameRequest(&head, original: exchange.effectiveHead.headers)
        if exchange.upstreamHTTP2 || typedHTTP2 {
            head.headers = HTTPHeaders(head.headers.map { ($0.name.lowercased(), $0.value) })
        }
        return head
    }

    func forward(_ part: HTTPServerRequestPart, for exchange: Exchange) {
        if active === exchange, let upstream, !exchange.earlyResponse || typedHTTP2 {
            write(part, exchange: exchange, to: upstream)
        } else {
            if exchange.controlSnapshot != nil, case let .body(buffer) = part {
                guard buffer.readableBytes <= 65_536 - exchange.pendingByteCount else {
                    controlFailed(exchange, error: HTTPControlError.capacityExceeded)
                    return
                }
                exchange.pendingByteCount += buffer.readableBytes
            }
            exchange.pending.append(part)
        }
    }

    func write(_ part: HTTPServerRequestPart, exchange: Exchange, to channel: Channel) {
        switch part {
        case .head:
            break
        case let .body(buffer):
            enqueueRequestBody(buffer, exchange: exchange, to: channel)
        case let .end(trailers):
            let end = HTTPClientRequestPart.end(trailers.map {
                Self.hopHeaders(
                    $0,
                    upgrading: false,
                    appendVia: false,
                    excluding: HTTP1MessageParser.tokens(Self.fields(exchange.head.headers), named: "connection")
                )
            })
            let complete = {
                if exchange.controlSnapshot != nil {
                    if case let .end(fields) = end, let fields, !fields.isEmpty {
                        (self.sink as? ObservationCaptureSink)?.receiveEffective(.requestTrailers(
                            requestID: exchange.id,
                            headers: Self.fields(fields)
                        ))
                    }
                    (self.sink as? ObservationCaptureSink)?.receiveEffective(.requestEnd(
                        requestID: exchange.id,
                        truncated: false
                    ))
                }
                if self.typedHTTP2, exchange.responseEnded {
                    channel.writeAndFlush(end).assumeIsolated().whenComplete { result in
                        switch result {
                        case .success: self.finish(exchange)
                        case .failure: self.abort(message: "HTTP/2 upload completion failed")
                        }
                    }
                } else {
                    channel.writeAndFlush(end, promise: nil)
                }
            }
            if let writer = exchange.requestFaultWriter {
                writer.finish(complete)
            } else {
                complete()
            }
        }
    }

    private func enqueueRequestBody(_ buffer: ByteBuffer, exchange: Exchange, to channel: Channel) {
        if let writer = exchange.requestFaultWriter {
            do {
                if exchange.requestEnded, exchange.controlSnapshot != nil {
                    try writer.enqueueRepresentation(buffer)
                } else {
                    try writer.enqueue(buffer)
                }
            } catch { controlFailed(exchange, error: error) }
        } else {
            writeRequestBody(buffer, exchange: exchange, to: channel)
        }
    }

    func writeRequestBody(_ buffer: ByteBuffer, exchange: Exchange, to channel: Channel) {
        if exchange.controlSnapshot != nil {
            (sink as? ObservationCaptureSink)?.receiveBody(
                requestID: exchange.id,
                direction: .request,
                representation: .effective,
                bytes: buffer.readableBytesView
            )
        }
        channel.writeAndFlush(HTTPClientRequestPart.body(.byteBuffer(buffer)), promise: nil)
    }

    func writeOriginHead(_ head: HTTPRequestHead, exchange: Exchange, channel: Channel) {
        if exchange.controlSnapshot != nil {
            publishEffectiveRequestHead(Self.fields(head.headers), exchange: exchange)
        }
        channel.write(HTTPClientRequestPart.head(head), promise: nil)
        let pending = exchange.pending
        exchange.pending.removeAll()
        exchange.pendingByteCount = 0
        for part in pending {
            if exchange.controlSnapshot == nil {
                captureRequest(part, exchange: exchange)
            }
            write(part, exchange: exchange, to: channel)
        }
        channel.flush()
        readOriginIfNeeded()
    }
}

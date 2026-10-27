import HTTPTypes
import NIOCore
import NIOHTTPTypes
import NIOQUICHelpers
import SwiftMITM

protocol ProbeMessagePart: Sendable {
    var retainedBytes: Int { get }
    var forwardedPayloadBytes: Int { get }
    var endsMessage: Bool { get }
    func compacted() -> Self
}

private func fieldBytes(_ fields: HTTPFields?, includeMetadata: Bool = true) -> Int {
    fields?.reduce(0) {
        $0 + (includeMetadata ? MemoryLayout<HTTPField>.stride : 0)
            + $1.name.rawName.utf8.count + $1.value.utf8.count
    } ?? 0
}

private func bodyCapacity(_ bytes: Int) -> Int {
    guard bytes > 0 else { return 0 }
    var capacity = 1
    while capacity < bytes { capacity *= 2 }
    return capacity
}

extension HTTPRequestPart: ProbeMessagePart {
    var forwardedPayloadBytes: Int {
        switch self {
        case let .head(head):
            return head.method.rawValue.utf8.count + (head.scheme?.utf8.count ?? 0)
                + (head.authority?.utf8.count ?? 0) + (head.path?.utf8.count ?? 0)
                + fieldBytes(head.headerFields, includeMetadata: false)
        case let .body(body): return body.readableBytes
        case let .end(trailers): return fieldBytes(trailers, includeMetadata: false)
        }
    }

    var retainedBytes: Int {
        switch self {
        case let .head(head):
            return head.method.rawValue.utf8.count + (head.scheme?.utf8.count ?? 0)
                + (head.authority?.utf8.count ?? 0) + (head.path?.utf8.count ?? 0) + fieldBytes(head.headerFields)
        case let .body(body): return bodyCapacity(body.readableBytes)
        case let .end(trailers): return fieldBytes(trailers)
        }
    }

    func compacted() -> HTTPRequestPart {
        if case let .body(body) = self {
            return .body(ByteBuffer(bytes: body.readableBytesView))
        }
        return self
    }

    var endsMessage: Bool {
        if case .end = self {
            true
        } else {
            false
        }
    }
}

extension HTTPResponsePart: ProbeMessagePart {
    var forwardedPayloadBytes: Int {
        switch self {
        case let .head(head): return 3 + fieldBytes(head.headerFields, includeMetadata: false)
        case let .body(body): return body.readableBytes
        case let .end(trailers): return fieldBytes(trailers, includeMetadata: false)
        }
    }

    var retainedBytes: Int {
        switch self {
        case let .head(head): return 3 + fieldBytes(head.headerFields)
        case let .body(body): return bodyCapacity(body.readableBytes)
        case let .end(trailers): return fieldBytes(trailers)
        }
    }

    func compacted() -> HTTPResponsePart {
        if case let .body(body) = self {
            return .body(ByteBuffer(bytes: body.readableBytesView))
        }
        return self
    }

    var endsMessage: Bool {
        if case .end = self {
            true
        } else {
            false
        }
    }
}

final class ProbeRelay<Part: ProbeMessagePart>: ChannelInboundHandler {
    typealias InboundIn = Part
    typealias Connect = (Part, Channel, NIOLoopBound<ProbeRelay<Part>>) -> EventLoopFuture<Channel>

    private var context: ChannelHandlerContext?
    private var destination: Channel?
    private var connect: Connect?
    private struct Pending {
        let part: Part
        let lease: ProbeStorageLease
    }

    private let metrics: ProbeMetrics
    private var queued: [Pending] = []
    private var pendingBytes = 0
    private var pendingStorageBytes = 0
    private var pendingWrites = 0
    private var connecting = false
    private var insideReadBatch = false
    private var inputEnded = false
    private var inputClosed = false
    private var closed = false
    private var sourceInactive = false
    var resumePeer: (() -> Void)?

    init(metrics: ProbeMetrics, destination: Channel? = nil, connect: Connect? = nil) {
        self.metrics = metrics
        self.destination = destination
        self.connect = connect
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        if context.channel.isActive {
            readIfReady()
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        context.fireChannelActive()
        readIfReady()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !closed, !inputEnded else { fail(); return }
        insideReadBatch = true
        let incoming = unwrapInboundIn(data)
        let bytes = incoming.forwardedPayloadBytes
        guard bytes <= 65_536 - pendingBytes else { fail(); return }
        let lease: ProbeStorageLease
        do {
            lease = try metrics.reserve(incoming.retainedBytes, kind: .queuedCopy)
        } catch {
            fail()
            return
        }
        let part = incoming.compacted()
        pendingBytes += bytes
        pendingStorageBytes += lease.bytes
        metrics.observeQueuedBytes(pendingStorageBytes, payload: pendingBytes)
        inputEnded = part.endsMessage
        if let destination {
            send(part, lease: lease, to: destination)
        } else {
            guard queued.count < 256 else {
                pendingBytes -= bytes
                pendingStorageBytes -= lease.bytes
                fail()
                return
            }
            queued.append(Pending(part: part, lease: lease))
            if !connecting {
                openDestination(first: part, context: context)
            }
        }
    }

    private func openDestination(first: Part, context: ChannelHandlerContext) {
        guard let connect else { fail(); return }
        self.connect = nil
        connecting = true
        let bound = NIOLoopBound(self, eventLoop: context.eventLoop)
        connect(first, context.channel, bound).assumeIsolated().whenComplete { result in
            self.connecting = false
            switch result {
            case let .success(destination):
                guard !self.closed else { destination.close(promise: nil); return }
                self.destination = destination
                let pending = self.queued
                self.queued.removeAll(keepingCapacity: false)
                for item in pending { self.send(item.part, lease: item.lease, to: destination) }
                self.readIfReady()
            case .failure: self.fail()
            }
        }
    }

    private func send(_ part: Part, lease: ProbeStorageLease, to destination: Channel) {
        let bytes = part.forwardedPayloadBytes
        pendingWrites += 1
        destination.writeAndFlush(part).assumeIsolated().whenComplete { result in
            withExtendedLifetime(lease) {}
            self.pendingBytes -= bytes
            self.pendingStorageBytes -= lease.bytes
            self.pendingWrites -= 1
            switch result {
            case .success:
                if self.sourceInactive, self.pendingWrites == 0 {
                    self.finish()
                } else {
                    let bound = NIOLoopBound(self, eventLoop: destination.eventLoop)
                    destination.eventLoop.execute { bound.value.readIfReady() }
                }
            case .failure: self.fail()
            }
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        insideReadBatch = false
        readIfReady()
        context.fireChannelReadComplete()
    }

    func readIfReady() {
        guard !closed, !inputClosed, !insideReadBatch, !connecting, pendingWrites == 0,
              let context, context.channel.isActive else { return }
        if let destination, !destination.isWritable {
            return
        }
        context.read()
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        if context.channel.isWritable {
            resumePeer?()
        }
        context.fireChannelWritabilityChanged()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event as? ChannelEvent == .inputClosed {
            inputClosed = true
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        sourceInactive = true
        if !inputEnded {
            fail()
        } else if pendingWrites == 0 {
            finish()
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { fail() }

    private func fail() {
        guard !closed else { return }
        finish()
        if let source = context?.channel { Self.abort(source) }
        if let destination { Self.abort(destination) }
    }

    private static func abort(_ channel: Channel) {
        // H3_REQUEST_CANCELLED: failure must not become a graceful empty response.
        guard let code = QUICApplicationErrorCode(0x10C) else {
            preconditionFailure("HTTP/3 cancellation code must fit a QUIC variable-length integer")
        }
        channel.triggerUserOutboundEvent(QUICResetStreamEvent(code: code), promise: nil)
        channel.triggerUserOutboundEvent(QUICStopSendingEvent(code: code), promise: nil)
    }

    private func finish() {
        guard !closed else { return }
        closed = true
        let bytes = queued.reduce(0) { $0 + $1.part.forwardedPayloadBytes }
        let storageBytes = queued.reduce(0) { $0 + $1.lease.bytes }
        queued.removeAll(keepingCapacity: false)
        pendingBytes -= bytes
        pendingStorageBytes -= storageBytes
        connect = nil
        resumePeer = nil
    }
}

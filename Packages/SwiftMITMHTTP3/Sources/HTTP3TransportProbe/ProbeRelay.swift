import HTTPTypes
import NIOCore
import NIOHTTPTypes
import SwiftMITM

protocol ProbeMessagePart: Sendable {
    var retainedBytes: Int { get }
    var endsMessage: Bool { get }
}

private func fieldBytes(_ fields: HTTPFields?) -> Int {
    fields?.reduce(0) { $0 + $1.name.rawName.utf8.count + $1.value.utf8.count } ?? 0
}

extension HTTPRequestPart: ProbeMessagePart {
    var retainedBytes: Int {
        switch self {
        case let .head(head):
            return head.method.rawValue.utf8.count + (head.scheme?.utf8.count ?? 0)
                + (head.authority?.utf8.count ?? 0) + (head.path?.utf8.count ?? 0) + fieldBytes(head.headerFields)
        case let .body(body): return body.readableBytes
        case let .end(trailers): return fieldBytes(trailers)
        }
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
    var retainedBytes: Int {
        switch self {
        case let .head(head): return fieldBytes(head.headerFields)
        case let .body(body): return body.readableBytes
        case let .end(trailers): return fieldBytes(trailers)
        }
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
    private let account: ResourceRetainedByteAccount
    private var queued: [Part] = []
    private var pendingBytes = 0
    private var pendingWrites = 0
    private var connecting = false
    private var insideReadBatch = false
    private var inputEnded = false
    private var inputClosed = false
    private var closed = false
    private var sourceInactive = false
    var resumePeer: (() -> Void)?

    init(admission: ResourceAdmissionController, destination: Channel? = nil, connect: Connect? = nil) {
        account = admission.makeRetainedByteAccount()
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
        let part = unwrapInboundIn(data)
        let bytes = part.retainedBytes
        guard bytes <= 65_536 - pendingBytes, account.reserve(bytes) else { fail(); return }
        pendingBytes += bytes
        inputEnded = part.endsMessage
        if let destination {
            send(part, to: destination)
        } else {
            guard queued.count < 256 else { account.release(bytes); pendingBytes -= bytes; fail(); return }
            queued.append(part)
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
                for part in pending { self.send(part, to: destination) }
                self.readIfReady()
            case .failure: self.fail()
            }
        }
    }

    private func send(_ part: Part, to destination: Channel) {
        let bytes = part.retainedBytes
        pendingWrites += 1
        let account = account
        destination.writeAndFlush(part).assumeIsolated().whenComplete { result in
            account.release(bytes)
            self.pendingBytes -= bytes
            self.pendingWrites -= 1
            switch result {
            case .success:
                if self.sourceInactive, self.pendingWrites == 0 {
                    self.finish()
                    destination.close(promise: nil)
                } else {
                    self.readIfReady()
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
        if !inputEnded || pendingWrites == 0 {
            finish()
            destination?.close(promise: nil)
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { fail() }

    private func fail() {
        finish()
        context?.close(promise: nil)
        destination?.close(promise: nil)
    }

    private func finish() {
        guard !closed else { return }
        closed = true
        let bytes = queued.reduce(0) { $0 + $1.retainedBytes }
        queued.removeAll(keepingCapacity: false)
        pendingBytes -= bytes
        account.release(bytes)
        connect = nil
        resumePeer = nil
    }
}

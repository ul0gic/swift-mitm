import NIOCore
import NIOTLS

struct NegotiatedUpstreamConnection: Sendable {
    let channel: Channel
    let applicationProtocol: ALPNProtocol
    let alpnHandler: NIOLoopBound<UpstreamALPNHandler>

    func releaseBufferedReads() -> EventLoopFuture<Void> {
        alpnHandler.value.releaseBufferedReads()
    }
}

enum ProxyALPNError: Error {
    case channelClosedBeforeNegotiation
    case malformedClientHello
    case noSupportedClientProtocol
    case protocolMismatch(downstream: String, upstream: String)
    case unsupportedProtocol(String)
}

enum ClientALPNOffer: Sendable {
    case absent
    case protocols([ALPNProtocol])
}

final class UpstreamALPNHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private static let maximumBufferedBytes = 65_536

    private let negotiatedProtocol: EventLoopPromise<ALPNProtocol>
    private var context: ChannelHandlerContext?
    private var reads: [ByteBuffer] = []
    private var bufferedByteCount = 0
    private var readComplete = false
    private var completed = false
    private var released = false

    init(negotiatedProtocol: EventLoopPromise<ALPNProtocol>) {
        self.negotiatedProtocol = negotiatedProtocol
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.context = nil
        reads.removeAll(keepingCapacity: false)
        bufferedByteCount = 0
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !released else {
            context.fireChannelRead(data)
            return
        }
        let buffer = unwrapInboundIn(data)
        bufferedByteCount += buffer.readableBytes
        guard bufferedByteCount <= Self.maximumBufferedBytes else {
            fail(ProxyALPNError.channelClosedBeforeNegotiation, context: context)
            context.close(promise: nil)
            return
        }
        reads.append(buffer)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        guard !released else {
            context.fireChannelReadComplete()
            return
        }
        readComplete = true
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let protocolName) = event {
            let result: Result<ALPNProtocol, Error>
            if let protocolName {
                result = ALPNProtocol(rawValue: protocolName)
                    .map(Result.success)
                    ?? .failure(ProxyALPNError.unsupportedProtocol(protocolName))
            } else {
                result = .success(.http11)
            }
            switch result {
            case .success(let negotiated):
                guard !completed else { break }
                completed = true
                context.channel.setOption(ChannelOptions.autoRead, value: false)
                    .map { negotiated }
                    .cascade(to: negotiatedProtocol)
            case .failure(let error):
                fail(error, context: context)
            }
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        fail(ProxyALPNError.channelClosedBeforeNegotiation, context: context)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error, context: context)
        context.fireErrorCaught(error)
    }

    func releaseBufferedReads() -> EventLoopFuture<Void> {
        guard let context else {
            return negotiatedProtocol.futureResult.eventLoop.makeFailedFuture(
                ProxyALPNError.channelClosedBeforeNegotiation
            )
        }
        released = true
        reads.forEach { context.fireChannelRead(wrapInboundOut($0)) }
        if readComplete {
            context.fireChannelReadComplete()
        }
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
        return context.channel.setOption(ChannelOptions.autoRead, value: true)
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        guard !completed else { return }
        completed = true
        negotiatedProtocol.fail(error)
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
    }
}

final class PendingTLSReads: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    let clientALPNOffer: EventLoopFuture<ClientALPNOffer>

    private let clientALPNOfferPromise: EventLoopPromise<ClientALPNOffer>
    private var reads: [ByteBuffer] = []
    private var bufferedByteCount = 0
    private var readComplete = false
    private var released = false
    private var completed = false

    init(eventLoop: EventLoop) {
        clientALPNOfferPromise = eventLoop.makePromise()
        clientALPNOffer = clientALPNOfferPromise.futureResult
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !released else {
            context.fireChannelRead(data)
            return
        }
        let buffer = unwrapInboundIn(data)
        bufferedByteCount += buffer.readableBytes
        guard bufferedByteCount <= ClientHelloALPNParser.maximumClientHelloBytes else {
            failIfPending(ProxyALPNError.malformedClientHello)
            context.close(promise: nil)
            return
        }
        reads.append(buffer)
        resolveClientALPNOffer(context: context)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        guard !released else {
            context.fireChannelReadComplete()
            return
        }
        readComplete = true
    }

    func replay(on pipeline: ChannelPipeline) {
        released = true
        reads.forEach { pipeline.fireChannelRead($0) }
        if readComplete {
            pipeline.fireChannelReadComplete()
        }
        reads.removeAll(keepingCapacity: false)
        bufferedByteCount = 0
        readComplete = false
    }

    func channelInactive(context: ChannelHandlerContext) {
        failIfPending(ProxyALPNError.channelClosedBeforeNegotiation)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        failIfPending(error)
        context.fireErrorCaught(error)
    }

    private func resolveClientALPNOffer(context: ChannelHandlerContext) {
        guard !completed else { return }
        let bytes = reads.flatMap { Array($0.readableBytesView) }
        do {
            guard let offer = try ClientHelloALPNParser.parse(bytes) else { return }
            completed = true
            context.channel.setOption(ChannelOptions.autoRead, value: false)
                .map { offer }
                .cascade(to: clientALPNOfferPromise)
        } catch {
            failIfPending(error)
            context.close(promise: nil)
        }
    }

    private func failIfPending(_ error: Error) {
        guard !completed else { return }
        completed = true
        clientALPNOfferPromise.fail(error)
    }
}

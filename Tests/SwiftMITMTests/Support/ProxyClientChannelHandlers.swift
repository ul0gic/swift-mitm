import NIOCore
import NIOTLS

final class TLSHandshakeProbe: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let promise: EventLoopPromise<String?>
    private var completed = false

    init(promise: EventLoopPromise<String?>) {
        self.promise = promise
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let negotiatedProtocol) = event {
            complete(.success(negotiatedProtocol))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        complete(.failure(ProxyTestError.tlsClosedBeforeHandshake))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        complete(.failure(error))
        context.close(promise: nil)
    }

    private func complete(_ result: Result<String?, Error>) {
        guard !completed else { return }
        completed = true
        promise.completeWith(result)
    }
}

final class ConnectResponseHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let promise: EventLoopPromise<Void>
    private var accumulated = ByteBuffer()

    init(promise: EventLoopPromise<Void>) {
        self.promise = promise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        accumulated.writeBuffer(&buffer)
        guard accumulated.readableBytesView.firstRange(of: [13, 10, 13, 10]) != nil else { return }
        let head = accumulated.getString(at: accumulated.readerIndex, length: accumulated.readableBytes) ?? ""
        if head.contains(" 200 ") {
            promise.succeed(())
        } else {
            promise.fail(ProxyTestError.connectFailed(head))
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        promise.fail(error)
        context.close(promise: nil)
    }
}

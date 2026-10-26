import NIOCore
import NIOHTTP1

final class ConnectHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let onEstablished: @Sendable (Channel, String) -> Void
    private let onRejected: @Sendable (CapturedConnectionFailureReason, ConnectionFailureCode) -> Void
    private var authority: String?
    private var terminal = false
    private var inputClosed = false

    init(
        onRejected: @escaping @Sendable (CapturedConnectionFailureReason, ConnectionFailureCode) -> Void = { _, _ in },
        onEstablished: @escaping @Sendable (Channel, String) -> Void
    ) {
        self.onEstablished = onEstablished
        self.onRejected = onRejected
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !terminal else { return }
        switch unwrapInboundIn(data) {
        case .head(let head):
            guard head.method == .CONNECT else {
                onRejected(.transportFailure, .unsupportedProtocol)
                respond(context: context, status: .methodNotAllowed)
                return
            }
            authority = head.uri
        case .body:
            break
        case .end:
            guard let authority, Self.isValidAuthority(authority) else {
                onRejected(.destinationUnavailable, .destinationUnavailable)
                respond(context: context, status: .badRequest)
                return
            }
            establishTunnel(context: context, authority: authority)
        }
    }

    private func establishTunnel(context: ChannelHandlerContext, authority: String) {
        terminal = true
        let head = HTTPResponseHead(
            version: .http1_1,
            status: .custom(code: 200, reasonPhrase: "Connection Established")
        )
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        let channel = context.channel
        let onEstablished = onEstablished
        let onRejected = onRejected
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        context.writeAndFlush(wrapOutboundOut(.end(nil)))
            .assumeIsolated()
            .flatMap { context.pipeline.syncOperations.removeHandler(context: context) }
            .nonisolated()
            .whenComplete { result in
                switch result {
                case .success:
                    onEstablished(channel, authority)
                    if owner.value.inputClosed {
                        channel.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
                    }
                case .failure:
                    onRejected(.transportFailure, .transportFailure)
                    channel.close(promise: nil)
                }
            }
    }

    private func respond(context: ChannelHandlerContext, status: HTTPResponseStatus) {
        terminal = true
        let head = HTTPResponseHead(version: .http1_1, status: status)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        let channel = context.channel
        context.writeAndFlush(wrapOutboundOut(.end(nil))).whenComplete { _ in
            channel.close(promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !terminal {
            terminal = true
            onRejected(.cancelled, .transportFailure)
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        if !terminal {
            terminal = true
            onRejected(.transportFailure, .transportFailure)
        }
        context.close(promise: nil)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            inputClosed = true
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }

    static func isValidAuthority(_ authority: String) -> Bool {
        ConnectionTarget(explicitConnectAuthority: authority) != nil
    }
}

import NIOCore
import NIOHTTP1

final class ConnectHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let onEstablished: @Sendable (Channel, String) -> Void
    private var authority: String?

    init(onEstablished: @escaping @Sendable (Channel, String) -> Void) {
        self.onEstablished = onEstablished
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head(let head):
            guard head.method == .CONNECT else {
                respond(context: context, status: .methodNotAllowed)
                return
            }
            authority = head.uri
        case .body:
            break
        case .end:
            guard let authority, Self.isValidAuthority(authority) else {
                respond(context: context, status: .badRequest)
                return
            }
            establishTunnel(context: context, authority: authority)
        }
    }

    private func establishTunnel(context: ChannelHandlerContext, authority: String) {
        let head = HTTPResponseHead(
            version: .http1_1,
            status: .custom(code: 200, reasonPhrase: "Connection Established")
        )
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        let channel = context.channel
        let onEstablished = onEstablished
        context.writeAndFlush(wrapOutboundOut(.end(nil)))
            .assumeIsolated()
            .flatMap { context.pipeline.syncOperations.removeHandler(context: context) }
            .nonisolated()
            .whenComplete { _ in onEstablished(channel, authority) }
    }

    private func respond(context: ChannelHandlerContext, status: HTTPResponseStatus) {
        let head = HTTPResponseHead(version: .http1_1, status: status)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        let channel = context.channel
        context.writeAndFlush(wrapOutboundOut(.end(nil))).whenComplete { _ in
            channel.close(promise: nil)
        }
    }

    static func isValidAuthority(_ authority: String) -> Bool {
        guard !authority.isEmpty,
            !authority.utf8.contains(where: { $0 < 0x20 || $0 == 0x7F || $0 == UInt8(ascii: " ") }),
            let separator = authority.lastIndex(of: ":")
        else {
            return false
        }
        let host = authority[authority.startIndex..<separator]
        let port = authority[authority.index(after: separator)...]
        guard !host.isEmpty, !port.isEmpty, let value = Int(port), (1...65535).contains(value) else {
            return false
        }
        return true
    }
}

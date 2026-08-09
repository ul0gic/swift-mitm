import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix

final class TestOriginServer {
    private let group: EventLoopGroup
    private let bodySize: Int
    private let chunkSize: Int
    private var channel: Channel?

    init(group: EventLoopGroup, bodySize: Int, chunkSize: Int = 16 * 1024) {
        self.group = group
        self.bodySize = bodySize
        self.chunkSize = chunkSize
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }

    func start() throws {
        let bodySize = bodySize
        let chunkSize = chunkSize
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.configureHTTP2Pipeline(
                    mode: .server,
                    connectionConfiguration: .init(),
                    streamConfiguration: .init()
                ) { stream in
                    stream.eventLoop.makeCompletedFuture {
                        try stream.pipeline.syncOperations.addHandler(
                            StreamingResponder(bodySize: bodySize, chunkSize: chunkSize)
                        )
                    }
                }
                .map { _ in () }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        try? channel?.close().wait()
    }
}

final class StreamingResponder: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private var remaining: Int
    private let chunkSize: Int
    private var headersSent = false
    private var finished = false

    init(bodySize: Int, chunkSize: Int) {
        self.remaining = bodySize
        self.chunkSize = chunkSize
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let payload = unwrapInboundIn(data)
        guard case .headers = payload, !headersSent else { return }
        headersSent = true
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: "200")
        headers.add(name: "content-type", value: "application/octet-stream")
        let head = HTTP2Frame.FramePayload.headers(.init(headers: headers))
        context.writeAndFlush(wrapOutboundOut(head), promise: nil)
        pump(context: context)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        if context.channel.isWritable {
            pump(context: context)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }

    private func pump(context: ChannelHandlerContext) {
        while remaining > 0 && context.channel.isWritable {
            let count = min(chunkSize, remaining)
            var buffer = context.channel.allocator.buffer(capacity: count)
            buffer.writeRepeatingByte(0x41, count: count)
            let frame = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(buffer), endStream: false))
            context.write(wrapOutboundOut(frame), promise: nil)
            remaining -= count
        }
        context.flush()
        if remaining == 0 && !finished {
            finished = true
            let empty = context.channel.allocator.buffer(capacity: 0)
            let end = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(empty), endStream: true))
            context.writeAndFlush(wrapOutboundOut(end), promise: nil)
        }
    }
}

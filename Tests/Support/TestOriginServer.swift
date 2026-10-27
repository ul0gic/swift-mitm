import NIOCore
import NIOHPACK
import NIOHTTP2

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

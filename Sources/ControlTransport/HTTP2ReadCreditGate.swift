import NIOCore
import NIOHTTP2

final class HTTP2ReadCreditGate: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame
    typealias InboundOut = HTTP2Frame
    typealias OutboundIn = HTTP2Frame
    typealias OutboundOut = HTTP2Frame
    private struct Credit {
        var reading = false
        var increment = 0
    }
    private var streams: [HTTP2StreamID: Credit] = [:]
    private var context: ChannelHandlerContext?

    func handlerAdded(context: ChannelHandlerContext) { self.context = context }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        if case .headers = frame.payload, streams[frame.streamID] == nil {
            streams[frame.streamID] = Credit()
        }
        context.fireChannelRead(data)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let frame = unwrapOutboundIn(data)
        guard frame.streamID != .rootStream, case .windowUpdate(let increment) = frame.payload,
              var credit = streams[frame.streamID], !credit.reading else {
            context.write(data, promise: promise)
            return
        }
        // NIO's uncommitted updates describe overlapping deficits, not additive credit.
        credit.increment = max(credit.increment, increment)
        streams[frame.streamID] = credit
        promise?.succeed(())
    }

    func pause(_ streamID: HTTP2StreamID) {
        guard var credit = streams[streamID] else { return }
        credit.reading = false
        streams[streamID] = credit
    }

    func read(_ streamID: HTTP2StreamID) {
        guard var credit = streams[streamID], let context else { return }
        let increment = credit.increment
        credit.increment = 0
        credit.reading = true
        streams[streamID] = credit
        if increment > 0 {
            context.writeAndFlush(NIOAny(HTTP2Frame(
                streamID: streamID, payload: .windowUpdate(windowSizeIncrement: increment)
            )), promise: nil)
        }
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let closed = event as? StreamClosedEvent { streams.removeValue(forKey: closed.streamID) }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        streams.removeAll()
        context.fireChannelInactive()
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        streams.removeAll()
        self.context = nil
    }
}

final class HTTP2StreamReadCreditHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias InboundOut = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload
    private let gate: NIOLoopBound<HTTP2ReadCreditGate>
    private var streamID: HTTP2StreamID?

    init(gate: NIOLoopBound<HTTP2ReadCreditGate>) { self.gate = gate }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let streamID {
            gate.value.pause(streamID)
            context.fireChannelRead(data)
            return
        }
        context.channel.getOption(HTTP2StreamChannelOptions.streamID).assumeIsolated().whenComplete { result in
            switch result {
            case .success(let streamID):
                self.streamID = streamID
                self.gate.value.pause(streamID)
                context.fireChannelRead(data)
            case .failure(let error): context.fireErrorCaught(error)
            }
        }
    }

    func read(context: ChannelHandlerContext) {
        if let streamID { gate.value.read(streamID) }
        context.read()
    }
}

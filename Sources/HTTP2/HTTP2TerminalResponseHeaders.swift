import NIOCore
import NIOHTTP2

final class HTTP2TerminalResponseHeaders {
    var originHeadEndsStream = false
}

final class HTTP2TerminalResponseObserver: ChannelInboundHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    private let state: NIOLoopBound<HTTP2TerminalResponseHeaders>

    init(state: NIOLoopBound<HTTP2TerminalResponseHeaders>) {
        self.state = state
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if case let .headers(frame) = unwrapInboundIn(data),
           let status = frame.headers.first(name: ":status"),
           let code = Int(status), code >= 200 {
            state.value.originHeadEndsStream = frame.endStream
        }
        context.fireChannelRead(data)
    }
}

final class HTTP2TerminalResponseWriter: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private struct PendingHead {
        var frame: HTTP2Frame.FramePayload.Headers
        let promise: EventLoopPromise<Void>?
        let control: HTTPControlByteReservation?
        let admission: ResourceReservation?
    }

    private let state: NIOLoopBound<HTTP2TerminalResponseHeaders>
    private let runtime: HTTPControlRuntime?
    private let admission: ResourceAdmissionController?
    private var pending: PendingHead?
    private var stopped = false

    init(
        state: NIOLoopBound<HTTP2TerminalResponseHeaders>,
        runtime: HTTPControlRuntime?,
        admission: ResourceAdmissionController?
    ) {
        self.state = state
        self.runtime = runtime
        self.admission = admission
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        guard !stopped else {
            promise?.fail(ChannelError.ioOnClosedChannel)
            return
        }
        let payload = unwrapOutboundIn(data)
        if pending != nil {
            if case let .data(frame) = payload, frame.endStream, frame.data.readableBytes == 0 {
                writePending(context: context, terminal: true, endPromise: promise)
                return
            }
            if case .rstStream = payload {
                discardPending(error: ChannelError.ioOnClosedChannel)
            } else {
                writePending(context: context, terminal: false, endPromise: nil)
            }
        } else if state.value.originHeadEndsStream,
                  case let .headers(frame) = payload,
                  let status = frame.headers.first(name: ":status"),
                  let code = Int(status), code >= 200 {
            state.value.originHeadEndsStream = false
            do {
                try retain(frame, promise: promise)
            } catch {
                stopped = true
                promise?.fail(error)
                context.fireErrorCaught(error)
                context.close(promise: nil)
            }
            return
        }
        context.write(data, promise: promise)
    }

    private func retain(_ frame: HTTP2Frame.FramePayload.Headers, promise: EventLoopPromise<Void>?) throws {
        // Match the selector's logical retained-frame charge; this is not a heap allocation estimate.
        let bytes = frame.headers.reduce(64) { $0 + $1.name.utf8.count + $1.value.utf8.count + 32 }
        let control = try runtime?.reserveTransportBytes(bytes)
        do {
            let reservation = try admission?.acquire(.retainedBytes(bytes))
            pending = PendingHead(frame: frame, promise: promise, control: control, admission: reservation)
        } catch {
            control?.release()
            throw error
        }
    }

    private func writePending(
        context: ChannelHandlerContext,
        terminal: Bool,
        endPromise: EventLoopPromise<Void>?
    ) {
        guard var head = pending else { return }
        pending = nil
        head.frame.endStream = terminal
        let completion = context.eventLoop.makePromise(of: Void.self)
        let control = head.control
        let reservation = head.admission
        completion.futureResult.whenComplete { _ in
            control?.release()
            reservation?.release()
        }
        completion.futureResult.cascade(to: head.promise)
        completion.futureResult.cascade(to: endPromise)
        context.write(wrapOutboundOut(.headers(head.frame)), promise: completion)
    }

    private func discardPending(error: Error) {
        guard let head = pending else { return }
        pending = nil
        head.control?.release()
        head.admission?.release()
        head.promise?.fail(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        stopped = true
        discardPending(error: ChannelError.ioOnClosedChannel)
        context.fireChannelInactive()
    }

    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        switch mode {
        case .all, .output:
            stopped = true
            discardPending(error: ChannelError.ioOnClosedChannel)
        case .input:
            break
        }
        context.close(mode: mode, promise: promise)
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        stopped = true
        discardPending(error: ChannelError.ioOnClosedChannel)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        stopped = true
        discardPending(error: error)
        context.fireErrorCaught(error)
    }
}

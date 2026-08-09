import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix

final class ClientConsumerState: @unchecked Sendable {
    private let lock = NIOLock()
    private var storedBytes = 0
    private var storedReadsEnabled = true

    var bytesReceived: Int { lock.withLock { storedBytes } }

    func add(_ count: Int) { lock.withLock { storedBytes += count } }

    var readsEnabled: Bool {
        get { lock.withLock { storedReadsEnabled } }
        set { lock.withLock { storedReadsEnabled = newValue } }
    }
}

final class TestHTTP2Client {
    private let group: EventLoopGroup
    let state = ClientConsumerState()

    private var connection: Channel?
    private var stream: Channel?
    private let donePromise: EventLoopPromise<Int>

    init(group: EventLoopGroup) {
        self.group = group
        self.donePromise = group.next().makePromise(of: Int.self)
    }

    var completion: EventLoopFuture<Int> { donePromise.futureResult }

    struct StallError: Error { let bytesReceived: Int }

    func armWatchdog(seconds: Int64) {
        let state = state
        let promise = donePromise
        group.next().scheduleTask(in: .seconds(seconds)) {
            promise.fail(StallError(bytesReceived: state.bytesReceived))
        }
    }

    func connectAndRequest(host: String, port: Int, authority: String, startPaused: Bool = false) throws {
        state.readsEnabled = !startPaused
        let connection = try ClientBootstrap(group: group)
            .connect(host: host, port: port)
            .flatMap { channel in
                channel.configureHTTP2Pipeline(
                    mode: .client,
                    connectionConfiguration: .init(),
                    streamConfiguration: .init()
                ) { $0.close() }
                .map { (channel, $0) }
            }
            .wait()

        self.connection = connection.0
        let state = state
        let promise = donePromise
        let stream = try connection.1.createStreamChannel { stream in
            stream.setOption(ChannelOptions.autoRead, value: false).flatMapThrowing {
                try stream.pipeline.syncOperations.addHandler(
                    ConsumingHandler(authority: authority, state: state, done: promise)
                )
            }
        }
        .wait()
        self.stream = stream
    }

    func pause() { state.readsEnabled = false }

    func resume() {
        state.readsEnabled = true
        guard let stream else { return }
        stream.eventLoop.execute { stream.read() }
    }

    func shutdown() {
        try? stream?.close().wait()
        try? connection?.close().wait()
    }
}

private final class ConsumingHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let state: ClientConsumerState
    private let done: EventLoopPromise<Int>
    private var finished = false

    init(authority: String, state: ClientConsumerState, done: EventLoopPromise<Int>) {
        self.authority = authority
        self.state = state
        self.done = done
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "GET")
        headers.add(name: ":path", value: "/stream")
        headers.add(name: ":scheme", value: "http")
        headers.add(name: ":authority", value: authority)
        let head = HTTP2Frame.FramePayload.headers(.init(headers: headers, endStream: true))
        context.writeAndFlush(wrapOutboundOut(head), promise: nil)
        if state.readsEnabled {
            context.read()
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let payload = unwrapInboundIn(data)
        switch payload {
        case .data(let frame):
            state.add(frame.data.readableBytes)
            if frame.endStream { complete() }
        case .headers(let frame):
            if frame.endStream { complete() }
        default:
            break
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        if state.readsEnabled {
            context.read()
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        complete()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        done.fail(error)
        context.close(promise: nil)
    }

    private func complete() {
        guard !finished else { return }
        finished = true
        done.succeed(state.bytesReceived)
    }
}

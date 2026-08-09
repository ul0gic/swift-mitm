import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP1
import NIOHTTP2

final class ProxyStalledFetch: @unchecked Sendable {
    let completion: EventLoopFuture<Int>

    private let connection: Channel
    private let readChannel: Channel
    private let state: ProxyStalledConsumerState

    init(
        connection: Channel,
        readChannel: Channel,
        state: ProxyStalledConsumerState,
        completion: EventLoopFuture<Int>
    ) {
        self.connection = connection
        self.readChannel = readChannel
        self.state = state
        self.completion = completion
    }

    var receivedBytes: Int { state.bytesReceived }

    func resume() async throws {
        if readChannel !== connection {
            try await enableReads(on: readChannel)
        }
        try await enableReads(on: connection)
    }

    func shutdown() async {
        if readChannel !== connection, readChannel.isActive {
            try? await readChannel.close().get()
        }
        if connection.isActive {
            try? await connection.close().get()
        }
    }

    private func enableReads(on channel: Channel) async throws {
        try await channel.setOption(ChannelOptions.autoRead, value: true).map { channel.read() }.get()
    }
}

final class ProxyStalledConsumerState: @unchecked Sendable {
    private let lock = NIOLock()
    private var storedBytes = 0

    var bytesReceived: Int { lock.withLock { storedBytes } }

    func add(_ count: Int) {
        lock.withLock { storedBytes += count }
    }
}

private final class ProxyStalledCompletion: @unchecked Sendable {
    let futureResult: EventLoopFuture<Int>

    private let lock = NIOLock()
    private let promise: EventLoopPromise<Int>
    private var completed = false

    init(eventLoop: EventLoop) {
        promise = eventLoop.makePromise()
        futureResult = promise.futureResult
    }

    func complete(_ result: Result<Int, Error>) {
        let shouldComplete = lock.withLock {
            guard !completed else { return false }
            completed = true
            return true
        }
        if shouldComplete {
            promise.completeWith(result)
        }
    }
}

extension ProxyTestClient {
    func beginStalledFetch(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        alpn: String,
        timeout: TimeAmount = .seconds(30)
    ) throws -> ProxyStalledFetch {
        let state = ProxyStalledConsumerState()
        let completion = ProxyStalledCompletion(eventLoop: group.next())
        let timeoutTask = completion.futureResult.eventLoop.scheduleTask(in: timeout) {
            completion.complete(.failure(ProxyTestError.stalledFetchTimeout(bytesReceived: state.bytesReceived)))
        }
        completion.futureResult.whenComplete { _ in timeoutTask.cancel() }
        let channel = try openTunnel(proxyPort: proxyPort, originHost: originHost, originPort: originPort)
        try startTLS(
            on: channel,
            serverHostname: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM,
            alpn: alpn
        )
        let authority = "\(originHost):\(originPort)"
        let readChannel = try makeReadChannel(
            channel: channel,
            alpn: alpn,
            authority: authority,
            state: state,
            completion: completion
        )
        return ProxyStalledFetch(
            connection: channel,
            readChannel: readChannel,
            state: state,
            completion: completion.futureResult
        )
    }

    private func makeReadChannel(
        channel: Channel,
        alpn: String,
        authority: String,
        state: ProxyStalledConsumerState,
        completion: ProxyStalledCompletion
    ) throws -> Channel {
        if alpn == "h2" {
            return try sendStalledHTTP2Request(
                on: channel,
                authority: authority,
                state: state,
                completion: completion
            )
        }
        try channel.setOption(ChannelOptions.autoRead, value: false).wait()
        let installFuture = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandlers([
                HTTPRequestEncoder(),
                ByteToMessageHandler(HTTPResponseDecoder()),
                H1StalledClient(authority: authority, state: state, completion: completion)
            ])
        }
        try installFuture.wait()
        return channel
    }

    private func sendStalledHTTP2Request(
        on channel: Channel,
        authority: String,
        state: ProxyStalledConsumerState,
        completion: ProxyStalledCompletion
    ) throws -> Channel {
        try channel.setOption(ChannelOptions.autoRead, value: false).wait()
        let muxFuture = channel.configureHTTP2Pipeline(
            mode: .client,
            connectionConfiguration: .init(),
            streamConfiguration: .init()
        ) { $0.close() }
        let mux = try muxFuture.wait()
        return try mux.createStreamChannel { stream in
            stream.setOption(ChannelOptions.autoRead, value: false).flatMapThrowing {
                try stream.pipeline.syncOperations.addHandler(
                    H2StalledClient(authority: authority, state: state, completion: completion)
                )
            }
        }
        .wait()
    }
}

private final class H1StalledClient: ChannelInboundHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart

    private let authority: String
    private let state: ProxyStalledConsumerState
    private let completion: ProxyStalledCompletion
    private var sent = false
    private var finished = false

    init(authority: String, state: ProxyStalledConsumerState, completion: ProxyStalledCompletion) {
        self.authority = authority
        self.state = state
        self.completion = completion
    }

    func handlerAdded(context: ChannelHandlerContext) {
        if context.channel.isActive {
            sendRequest(context: context)
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        sendRequest(context: context)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head:
            break
        case .body(let buffer):
            state.add(buffer.readableBytes)
        case .end:
            finish()
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !finished {
            completion.complete(.failure(ProxyTestError.connectionClosedBeforeResponseEnd))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func sendRequest(context: ChannelHandlerContext) {
        guard !sent else { return }
        sent = true
        var headers = HTTPHeaders()
        headers.add(name: "host", value: authority)
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: "/stream", headers: headers)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }

    private func finish() {
        guard !finished else { return }
        finished = true
        completion.complete(.success(state.bytesReceived))
    }
}

private final class H2StalledClient: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let state: ProxyStalledConsumerState
    private let completion: ProxyStalledCompletion
    private var finished = false

    init(authority: String, state: ProxyStalledConsumerState, completion: ProxyStalledCompletion) {
        self.authority = authority
        self.state = state
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "GET")
        headers.add(name: ":path", value: "/stream")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":authority", value: authority)
        context.writeAndFlush(wrapOutboundOut(.headers(.init(headers: headers, endStream: true))), promise: nil)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .data(let frame):
            state.add(frame.data.readableBytes)
            if frame.endStream {
                finish()
            }
        case .headers(let frame):
            if frame.endStream {
                finish()
            }
        default:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !finished {
            completion.complete(.failure(ProxyTestError.connectionClosedBeforeResponseEnd))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func finish() {
        guard !finished else { return }
        finished = true
        completion.complete(.success(state.bytesReceived))
    }
}

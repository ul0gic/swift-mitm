import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

enum Phase6IngressAdapterError: Error {
    case invalidConnectRequest
    case requestTooLarge
}

final class Phase6IngressAdapter {
    private let group: EventLoopGroup
    private let proxyPort: Int
    private let destinationPort: Int
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var listener: Channel?

    init(group: EventLoopGroup, proxyPort: Int, destinationPort: Int) {
        self.group = group
        self.proxyPort = proxyPort
        self.destinationPort = destinationPort
    }

    var localPort: Int { listener?.localAddress?.port ?? 0 }

    func start() throws {
        let children = children
        let proxyPort = proxyPort
        let destinationPort = destinationPort
        listener = try phase4BoundedWait(ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .childChannelOption(ChannelOptions.autoRead, value: false)
            .childChannelInitializer { channel in
                Self.register(channel, in: children)
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase6ConnectTerminator(
                        proxyPort: proxyPort,
                        destinationPort: destinationPort,
                        children: children
                    ))
                }
            }
            .bind(host: "127.0.0.1", port: 0))
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? phase4BoundedWait($0.close()) }
        if let listener {
            try? phase4BoundedWait(listener.close())
        }
    }

    fileprivate static func register(
        _ channel: Channel,
        in children: NIOLockedValueBox<[ObjectIdentifier: Channel]>
    ) {
        let identifier = ObjectIdentifier(channel)
        children.withLockedValue { $0[identifier] = channel }
        channel.closeFuture.whenComplete { _ in
            children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
        }
    }
}

private final class Phase6ConnectTerminator: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private static let maximumRequestBytes = 4_096
    private static let delimiter: [UInt8] = [13, 10, 13, 10]
    private static let response = ByteBuffer(string: "HTTP/1.1 200 Connection Established\r\n\r\n")

    private let proxyPort: Int
    private let destinationPort: Int
    private let children: NIOLockedValueBox<[ObjectIdentifier: Channel]>
    private var requestBytes: [UInt8] = []
    private var connecting = false

    init(
        proxyPort: Int,
        destinationPort: Int,
        children: NIOLockedValueBox<[ObjectIdentifier: Channel]>
    ) {
        self.proxyPort = proxyPort
        self.destinationPort = destinationPort
        self.children = children
    }

    func channelActive(context: ChannelHandlerContext) {
        context.read()
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !connecting else { return }
        requestBytes.append(contentsOf: unwrapInboundIn(data).readableBytesView)
        guard requestBytes.count <= Self.maximumRequestBytes else {
            fail(Phase6IngressAdapterError.requestTooLarge, context: context)
            return
        }
        guard let delimiter = requestBytes.firstRange(of: Self.delimiter) else {
            context.read()
            return
        }
        let request = Array(requestBytes[..<delimiter.upperBound])
        let applicationBytes = Array(requestBytes[delimiter.upperBound...])
        guard validConnectRequest(request) else {
            fail(Phase6IngressAdapterError.invalidConnectRequest, context: context)
            return
        }
        connecting = true
        connect(applicationBytes: applicationBytes, context: context)
    }

    func channelInactive(context: ChannelHandlerContext) {
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error, context: context)
    }

    private func validConnectRequest(_ bytes: [UInt8]) -> Bool {
        guard let request = String(bytes: bytes, encoding: .utf8) else { return false }
        let authority = "localhost:\(destinationPort)"
        return request == "CONNECT \(authority) HTTP/1.1\r\nHost: \(authority)\r\n\r\n"
    }

    private func connect(applicationBytes: [UInt8], context: ChannelHandlerContext) {
        let downstream = context.channel
        let eventLoop = context.eventLoop
        let owner = NIOLoopBound(self, eventLoop: eventLoop)
        let boundContext = NIOLoopBound(context, eventLoop: eventLoop)
        let children = children
        let destinationPort = destinationPort
        ClientBootstrap(group: downstream.eventLoop)
            .connectTimeout(.seconds(2))
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .channelOption(ChannelOptions.autoRead, value: false)
            .connect(host: "127.0.0.1", port: proxyPort)
            .flatMap { upstream in
                Phase6IngressAdapter.register(upstream, in: children)
                let handler = owner.value
                let context = boundContext.value
                return handler.installRelay(downstream: downstream, upstream: upstream, context: context)
                    .flatMap {
                        let header = Phase4ProxyV2Header.ipv4(
                            source: [192, 0, 2, 60],
                            destination: [127, 0, 0, 1],
                            sourcePort: 50_060,
                            destinationPort: destinationPort
                        )
                        upstream.write(ByteBuffer(bytes: header.bytes), promise: nil)
                        if !applicationBytes.isEmpty {
                            upstream.write(ByteBuffer(bytes: applicationBytes), promise: nil)
                        }
                        upstream.flush()
                        return downstream.writeAndFlush(Self.response)
                    }
                    .map {
                        downstream.read()
                        upstream.read()
                    }
                    .flatMapError { error in
                        upstream.close(promise: nil)
                        return upstream.eventLoop.makeFailedFuture(error)
                    }
            }
            .whenFailure { error in
                owner.value.fail(error, context: boundContext.value)
            }
    }

    private func installRelay(
        downstream: Channel,
        upstream: Channel,
        context: ChannelHandlerContext
    ) -> EventLoopFuture<Void> {
        let pair = Phase6RelayHandler.matchedPair()
        do {
            context.pipeline.syncOperations.removeHandler(context: context, promise: nil)
            try downstream.pipeline.syncOperations.addHandler(pair.downstream)
            try upstream.pipeline.syncOperations.addHandler(pair.upstream)
            return context.eventLoop.makeSucceededVoidFuture()
        } catch {
            return context.eventLoop.makeFailedFuture(error)
        }
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        context.fireErrorCaught(error)
        context.close(promise: nil)
    }
}

private final class Phase6RelayHandler {
    private var partner: Phase6RelayHandler?
    private var context: ChannelHandlerContext?
    private var pendingRead = false
    private var writable = false
    private var inputEnded = false

    static func matchedPair() -> (downstream: Phase6RelayHandler, upstream: Phase6RelayHandler) {
        let downstream = Phase6RelayHandler()
        let upstream = Phase6RelayHandler()
        downstream.partner = upstream
        upstream.partner = downstream
        return (downstream, upstream)
    }

    private func updateWritability(_ writable: Bool) {
        self.writable = writable
        if writable {
            partner?.resumeRead()
        }
    }

    private func resumeRead() {
        guard pendingRead else { return }
        pendingRead = false
        context?.read()
    }

    private var partnerWritable: Bool { partner?.writable ?? false }
}

extension Phase6RelayHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        updateWritability(context.channel.isWritable)
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        writable = false
        self.context = nil
        partner = nil
    }

    func channelActive(context: ChannelHandlerContext) {
        updateWritability(context.channel.isWritable)
        context.fireChannelActive()
        if partnerWritable {
            context.read()
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard let partnerContext = partner?.context else {
            context.close(promise: nil)
            return
        }
        partnerContext.write(NIOAny(unwrapInboundIn(data)), promise: nil)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        partner?.context?.flush()
        if partnerWritable {
            context.read()
        } else {
            pendingRead = true
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        partner?.context?.close(promise: nil)
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            guard !inputEnded else { return }
            inputEnded = true
            partner?.context?.close(mode: .output, promise: nil)
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
        partner?.context?.close(promise: nil)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        updateWritability(context.channel.isWritable)
        context.fireChannelWritabilityChanged()
    }

    func read(context: ChannelHandlerContext) {
        if partnerWritable {
            context.read()
        } else {
            pendingRead = true
        }
    }
}

final class Phase6TerminalPeer: Sendable {
    private let group: EventLoopGroup
    private let listener = NIOLockedValueBox<Channel?>(nil)
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private let acceptedCompletion: Phase2FixtureCompletion<Void>

    init(group: EventLoopGroup) {
        self.group = group
        acceptedCompletion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { listener.withLockedValue { $0?.localAddress?.port ?? 0 } }
    var accepted: EventLoopFuture<Void> { acceptedCompletion.futureResult }

    func start() throws {
        let children = children
        let acceptedCompletion = acceptedCompletion
        let channel = try phase4BoundedWait(ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                Phase6IngressAdapter.register(channel, in: children)
                acceptedCompletion.complete(.success(()))
                return channel.eventLoop.makeSucceededVoidFuture()
            }
            .bind(host: "127.0.0.1", port: 0))
        listener.withLockedValue { $0 = channel }
    }

    func closeAll() {
        children.withLockedValue { Array($0.values) }.forEach { try? phase4BoundedWait($0.close()) }
        if let channel = listener.withLockedValue({ $0 }) {
            try? phase4BoundedWait(channel.close())
        }
        acceptedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

final class Phase6TerminalClient: Sendable {
    private let group: EventLoopGroup
    private let channel = NIOLockedValueBox<Channel?>(nil)

    init(group: EventLoopGroup) {
        self.group = group
    }

    func connect(port: Int, bytes: [UInt8]) throws {
        let connected = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .connect(host: "127.0.0.1", port: port))
        channel.withLockedValue { $0 = connected }
        try phase4BoundedWait(connected.writeAndFlush(ByteBuffer(bytes: bytes)))
    }

    func close() {
        if let channel = channel.withLockedValue({ $0 }) {
            try? phase4BoundedWait(channel.close())
        }
    }
}

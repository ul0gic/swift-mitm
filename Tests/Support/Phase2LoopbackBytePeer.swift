import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

enum Phase2FixtureError: Error, Equatable {
    case closedBeforeExpectedBytes
    case deadlineExceeded
    case exceededByteLimit
    case unexpectedBytes
}

final class Phase2ByteObservation: @unchecked Sendable {
    let futureResult: EventLoopFuture<[UInt8]>

    private let lock = NIOLock()
    private let completion: Phase2FixtureCompletion<[UInt8]>
    private let expectedBytes: [UInt8]
    private let maximumBytes: Int
    private var bytes: [UInt8] = []
    private var completed = false

    init(eventLoop: EventLoop, expectedBytes: [UInt8], maximumBytes: Int) {
        self.completion = Phase2FixtureCompletion(eventLoop: eventLoop)
        self.futureResult = completion.futureResult
        self.expectedBytes = expectedBytes
        self.maximumBytes = maximumBytes
        if expectedBytes.isEmpty {
            completed = true
            completion.complete(.success([]))
        }
    }

    func append(_ incoming: [UInt8]) {
        let result = lock.withLock { () -> Result<[UInt8], Error>? in
            guard !completed else { return nil }
            bytes += incoming
            guard bytes.count <= maximumBytes else {
                completed = true
                return .failure(Phase2FixtureError.exceededByteLimit)
            }
            guard bytes.count >= expectedBytes.count else { return nil }
            completed = true
            return bytes == expectedBytes ? .success(bytes) : .failure(Phase2FixtureError.unexpectedBytes)
        }
        if let result {
            completion.complete(result)
        }
    }

    func closed() {
        let shouldFail = lock.withLock { () -> Bool in
            guard !completed else { return false }
            completed = true
            return true
        }
        if shouldFail {
            completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
    }
}

final class Phase2LoopbackBytePeer {
    struct Configuration: Sendable {
        let initialBytes: [UInt8]
        let expectedBytes: [UInt8]
        let replyBytes: [UInt8]
        let closesOutputAfterInitialBytes: Bool
        let maximumInboundBytes: Int

        init(
            initialBytes: [UInt8] = [],
            expectedBytes: [UInt8],
            replyBytes: [UInt8] = [],
            closesOutputAfterInitialBytes: Bool = false,
            maximumInboundBytes: Int = 64 * 1_024
        ) {
            self.initialBytes = initialBytes
            self.expectedBytes = expectedBytes
            self.replyBytes = replyBytes
            self.closesOutputAfterInitialBytes = closesOutputAfterInitialBytes
            self.maximumInboundBytes = maximumInboundBytes
        }
    }

    private let group: EventLoopGroup
    private let configuration: Configuration
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private let observationCompletion: Phase2FixtureCompletion<Phase2ByteObservation>
    private var channel: Channel?

    init(group: EventLoopGroup, configuration: Configuration) {
        self.group = group
        self.configuration = configuration
        self.observationCompletion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var observation: EventLoopFuture<Phase2ByteObservation> { observationCompletion.futureResult }

    func start() throws {
        let configuration = configuration
        let children = children
        let observationCompletion = observationCompletion
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .childChannelInitializer { channel in
                let observation = Phase2ByteObservation(
                    eventLoop: channel.eventLoop,
                    expectedBytes: configuration.expectedBytes,
                    maximumBytes: configuration.maximumInboundBytes
                )
                observationCompletion.complete(.success(observation))
                let identifier = ObjectIdentifier(channel)
                children.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        Phase2LoopbackServerHandler(configuration: configuration, observation: observation)
                    )
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
        try? channel?.close().wait()
        observationCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase2LoopbackServerHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let configuration: Phase2LoopbackBytePeer.Configuration
    private let observation: Phase2ByteObservation
    private var receivedByteCount = 0
    private var replied = false

    init(configuration: Phase2LoopbackBytePeer.Configuration, observation: Phase2ByteObservation) {
        self.configuration = configuration
        self.observation = observation
    }

    func channelActive(context: ChannelHandlerContext) {
        guard !configuration.initialBytes.isEmpty else { return }
        let write = context.writeAndFlush(NIOAny(ByteBuffer(bytes: configuration.initialBytes)))
        if configuration.closesOutputAfterInitialBytes {
            let loopBoundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
            write.whenComplete { _ in loopBoundContext.value.close(mode: .output, promise: nil) }
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        receivedByteCount += buffer.readableBytes
        observation.append(Array(buffer.readableBytesView))
        guard
            !replied,
            !configuration.replyBytes.isEmpty,
            receivedByteCount >= configuration.expectedBytes.count
        else { return }
        replied = true
        context.writeAndFlush(NIOAny(ByteBuffer(bytes: configuration.replyBytes)), promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        observation.closed()
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        observation.closed()
        context.close(promise: nil)
    }
}

final class Phase2LoopbackByteClient {
    private let group: EventLoopGroup
    private var channel: Channel?
    private var inboundCompletion: Phase2FixtureCompletion<[UInt8]>?

    init(group: EventLoopGroup) {
        self.group = group
    }

    var inboundBytes: EventLoopFuture<[UInt8]>? { inboundCompletion?.futureResult }

    func connect(port: Int, autoRead: Bool = true, expectedInboundBytes: [UInt8]? = nil) throws {
        let inboundCompletion = expectedInboundBytes.map { _ in
            Phase2FixtureCompletion<[UInt8]>(eventLoop: group.next())
        }
        self.inboundCompletion = inboundCompletion
        channel = try ClientBootstrap(group: group)
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .channelOption(ChannelOptions.autoRead, value: autoRead)
            .channelInitializer { channel in
                guard let expectedInboundBytes, let inboundCompletion else {
                    return channel.eventLoop.makeSucceededVoidFuture()
                }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase2LoopbackClientHandler(
                        expectedBytes: expectedInboundBytes,
                        completion: inboundCompletion
                    ))
                }
            }
            .connect(host: "127.0.0.1", port: port)
            .wait()
    }

    func write(_ bytes: [UInt8], closeOutput: Bool = false) throws {
        guard let channel else { throw Phase2FixtureError.closedBeforeExpectedBytes }
        try channel.writeAndFlush(ByteBuffer(bytes: bytes)).wait()
        if closeOutput {
            try channel.close(mode: .output).wait()
        }
    }

    func resumeReads() throws {
        guard let channel else { throw Phase2FixtureError.closedBeforeExpectedBytes }
        try channel.setOption(ChannelOptions.autoRead, value: true).wait()
        channel.read()
    }

    func stop() {
        try? channel?.close().wait()
        inboundCompletion?.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase2LoopbackClientHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let expectedBytes: [UInt8]
    private let completion: Phase2FixtureCompletion<[UInt8]>
    private var bytes: [UInt8] = []

    init(expectedBytes: [UInt8], completion: Phase2FixtureCompletion<[UInt8]>) {
        self.expectedBytes = expectedBytes
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        bytes.append(contentsOf: buffer.readableBytesView)
        guard bytes.count <= expectedBytes.count else {
            completion.complete(.failure(Phase2FixtureError.exceededByteLimit))
            context.close(promise: nil)
            return
        }
        if bytes.count == expectedBytes.count {
            let result: Result<[UInt8], Error> = bytes == expectedBytes
                ? .success(bytes)
                : .failure(Phase2FixtureError.unexpectedBytes)
            completion.complete(result)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

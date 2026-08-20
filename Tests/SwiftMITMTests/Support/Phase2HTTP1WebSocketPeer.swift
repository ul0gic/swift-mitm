import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

final class Phase2HTTP1WebSocketPeer {
    static let requestBytes = Array([
        "GET /socket HTTP/1.1\r\n",
        "Host: localhost\r\n",
        "Connection: Upgrade\r\n",
        "Upgrade: websocket\r\n",
        "Sec-WebSocket-Version: 13\r\n",
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    ].joined().utf8)
    static let responseBytes = Array([
        "HTTP/1.1 101 Switching Protocols\r\n",
        "Connection: Upgrade\r\n",
        "Upgrade: websocket\r\n",
        "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
    ].joined().utf8)

    private let group: EventLoopGroup
    private let expectedClientFrame: [UInt8]
    private let serverFrame: [UInt8]
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private let receivedFrameCompletion: Phase2FixtureCompletion<[UInt8]>
    private var channel: Channel?

    init(group: EventLoopGroup, expectedClientFrame: [UInt8], serverFrame: [UInt8]) {
        self.group = group
        self.expectedClientFrame = expectedClientFrame
        self.serverFrame = serverFrame
        self.receivedFrameCompletion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var receivedFrame: EventLoopFuture<[UInt8]> { receivedFrameCompletion.futureResult }

    func start() throws {
        let expectedClientFrame = expectedClientFrame
        let serverFrame = serverFrame
        let receivedFrameCompletion = receivedFrameCompletion
        let children = children
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                children.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase2HTTP1WebSocketPeerHandler(
                        expectedClientFrame: expectedClientFrame,
                        serverFrame: serverFrame,
                        receivedFrameCompletion: receivedFrameCompletion
                    ))
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
        try? channel?.close().wait()
        receivedFrameCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase2HTTP1WebSocketPeerHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private static let maximumHandshakeBytes = 4_096

    private let expectedClientFrame: [UInt8]
    private let serverFrame: [UInt8]
    private let receivedFrameCompletion: Phase2FixtureCompletion<[UInt8]>
    private var bytes: [UInt8] = []
    private var upgraded = false
    private var completed = false

    init(
        expectedClientFrame: [UInt8],
        serverFrame: [UInt8],
        receivedFrameCompletion: Phase2FixtureCompletion<[UInt8]>
    ) {
        self.expectedClientFrame = expectedClientFrame
        self.serverFrame = serverFrame
        self.receivedFrameCompletion = receivedFrameCompletion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        bytes.append(contentsOf: buffer.readableBytesView)
        guard bytes.count <= Self.maximumHandshakeBytes + expectedClientFrame.count else {
            fail(Phase2FixtureError.exceededByteLimit, context: context)
            return
        }
        if !upgraded {
            receiveHandshake(context: context)
        }
        if upgraded {
            receiveFrame(context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !completed {
            completed = true
            receivedFrameCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error, context: context)
    }

    private func receiveHandshake(context: ChannelHandlerContext) {
        guard bytes.count >= Phase2HTTP1WebSocketPeer.requestBytes.count else { return }
        let request = Array(bytes.prefix(Phase2HTTP1WebSocketPeer.requestBytes.count))
        guard request == Phase2HTTP1WebSocketPeer.requestBytes else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        bytes.removeFirst(request.count)
        upgraded = true
        context.writeAndFlush(NIOAny(ByteBuffer(bytes: Phase2HTTP1WebSocketPeer.responseBytes)), promise: nil)
    }

    private func receiveFrame(context: ChannelHandlerContext) {
        guard bytes.count >= expectedClientFrame.count else { return }
        guard bytes == expectedClientFrame else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        completed = true
        receivedFrameCompletion.complete(.success(bytes))
        context.writeAndFlush(NIOAny(ByteBuffer(bytes: serverFrame)), promise: nil)
        bytes.removeAll(keepingCapacity: false)
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        if !completed {
            completed = true
            receivedFrameCompletion.complete(.failure(error))
        }
        context.close(promise: nil)
    }
}

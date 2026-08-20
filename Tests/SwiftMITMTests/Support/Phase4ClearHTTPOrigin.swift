import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

enum Phase4ClearHTTPScenario: Equatable, Sendable {
    case requestResponse
    case webSocket
}

struct Phase4ClearHTTPOriginResult: Sendable {
    let requestBytes: [UInt8]
    let clientWebSocketFrames: [UInt8]
}

final class Phase4ClearHTTPOrigin {
    static let requestBytes = Array("GET /clear HTTP/1.1\r\nHost: localhost\r\n\r\n".utf8)
    static let responseBody = Array("clear-response".utf8)
    static let responseBytes = Array(
        "HTTP/1.1 200 OK\r\nContent-Length: \(responseBody.count)\r\nConnection: close\r\n\r\n".utf8
    ) + responseBody

    private let group: EventLoopGroup
    private let scenario: Phase4ClearHTTPScenario
    private let completion: Phase2FixtureCompletion<Phase4ClearHTTPOriginResult>
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup, scenario: Phase4ClearHTTPScenario) {
        self.group = group
        self.scenario = scenario
        completion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var result: EventLoopFuture<Phase4ClearHTTPOriginResult> { completion.futureResult }

    func start() throws {
        let scenario = scenario
        let completion = completion
        let children = children
        channel = try phase4BoundedWait(ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                children.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase4ClearHTTPOriginHandler(
                        scenario: scenario,
                        completion: completion
                    ))
                }
            }
            .bind(host: "127.0.0.1", port: 0))
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? phase4BoundedWait($0.close()) }
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase4ClearHTTPOriginHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private static let maximumBytes = 4_096

    private let scenario: Phase4ClearHTTPScenario
    private let completion: Phase2FixtureCompletion<Phase4ClearHTTPOriginResult>
    private var bytes: [UInt8] = []
    private var requestBytes: [UInt8]?
    private var completed = false

    init(
        scenario: Phase4ClearHTTPScenario,
        completion: Phase2FixtureCompletion<Phase4ClearHTTPOriginResult>
    ) {
        self.scenario = scenario
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        bytes.append(contentsOf: unwrapInboundIn(data).readableBytesView)
        guard bytes.count <= Self.maximumBytes else {
            fail(Phase2FixtureError.exceededByteLimit, context: context)
            return
        }
        if requestBytes == nil {
            receiveRequest(context: context)
        }
        if requestBytes != nil, case .webSocket = scenario {
            receiveWebSocketFrames(context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !completed {
            fail(Phase2FixtureError.closedBeforeExpectedBytes, context: context)
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error, context: context)
    }

    private func receiveRequest(context: ChannelHandlerContext) {
        let expected = expectedRequest
        guard bytes.count >= expected.count else { return }
        let request = Array(bytes.prefix(expected.count))
        guard request == expected else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        requestBytes = request
        bytes.removeFirst(request.count)
        switch scenario {
        case .requestResponse:
            context.writeAndFlush(NIOAny(ByteBuffer(bytes: Phase4ClearHTTPOrigin.responseBytes)), promise: nil)
            complete(clientFrames: [])
        case .webSocket:
            context.writeAndFlush(NIOAny(ByteBuffer(bytes: Phase2HTTP1WebSocketPeer.responseBytes)), promise: nil)
        }
    }

    private func receiveWebSocketFrames(context: ChannelHandlerContext) {
        guard bytes.count >= WebSocketWire.clientFrames.count else { return }
        guard bytes == WebSocketWire.clientFrames else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        context.writeAndFlush(NIOAny(ByteBuffer(bytes: WebSocketWire.serverFrames)), promise: nil)
        complete(clientFrames: bytes)
    }

    private var expectedRequest: [UInt8] {
        switch scenario {
        case .requestResponse:
            Phase4ClearHTTPOrigin.requestBytes
        case .webSocket:
            Phase2HTTP1WebSocketPeer.requestBytes
        }
    }

    private func complete(clientFrames: [UInt8]) {
        guard !completed, let requestBytes else { return }
        completed = true
        completion.complete(.success(.init(
            requestBytes: requestBytes,
            clientWebSocketFrames: clientFrames
        )))
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        if !completed {
            completed = true
            completion.complete(.failure(error))
        }
        context.close(promise: nil)
    }
}

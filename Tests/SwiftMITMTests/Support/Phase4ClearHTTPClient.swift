import NIOCore
import NIOPosix

final class Phase4ClearHTTPClient {
    private let group: EventLoopGroup
    private var channel: Channel?
    private var completion: Phase2FixtureCompletion<[UInt8]>?

    init(group: EventLoopGroup) {
        self.group = group
    }

    func exchange(port: Int, scenario: Phase4ClearHTTPScenario) throws -> EventLoopFuture<[UInt8]> {
        let expected = expectedResponse(for: scenario)
        let completion = Phase2FixtureCompletion<[UInt8]>(eventLoop: group.next())
        self.completion = completion
        let channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase4ClearHTTPClientHandler(
                        scenario: scenario,
                        expectedBytes: expected,
                        completion: completion
                    ))
                }
            }
            .connect(host: "127.0.0.1", port: port))
        self.channel = channel
        let request = scenario == .requestResponse
            ? Phase4ClearHTTPOrigin.requestBytes
            : Phase2HTTP1WebSocketPeer.requestBytes
        try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: request)))
        return completion.futureResult
    }

    func stop() {
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        completion?.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }

    private func expectedResponse(for scenario: Phase4ClearHTTPScenario) -> [UInt8] {
        switch scenario {
        case .requestResponse:
            Phase4ClearHTTPOrigin.responseBytes
        case .webSocket:
            Phase2HTTP1WebSocketPeer.responseBytes + WebSocketWire.serverFrames
        }
    }
}

private final class Phase4ClearHTTPClientHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let scenario: Phase4ClearHTTPScenario
    private let expectedBytes: [UInt8]
    private let completion: Phase2FixtureCompletion<[UInt8]>
    private var bytes: [UInt8] = []
    private var sentFrames = false

    init(
        scenario: Phase4ClearHTTPScenario,
        expectedBytes: [UInt8],
        completion: Phase2FixtureCompletion<[UInt8]>
    ) {
        self.scenario = scenario
        self.expectedBytes = expectedBytes
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        bytes.append(contentsOf: unwrapInboundIn(data).readableBytesView)
        guard bytes.count <= expectedBytes.count, bytes == Array(expectedBytes.prefix(bytes.count)) else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        if case .webSocket = scenario,
           bytes.count >= Phase2HTTP1WebSocketPeer.responseBytes.count,
           !sentFrames {
            sentFrames = true
            context.writeAndFlush(NIOAny(ByteBuffer(bytes: WebSocketWire.clientFrames)), promise: nil)
        }
        if bytes.count == expectedBytes.count {
            completion.complete(.success(bytes))
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

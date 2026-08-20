import NIOCore
import NIOPosix

enum Phase4ProxyV2Delivery: Sendable {
    case coalesced
    case fragmented([Int])
}

struct Phase4ProxyV2Header: Equatable, Sendable {
    static let signature = Phase2ProxyV2Corpus.signature

    let bytes: [UInt8]

    static func ipv4(
        source: [UInt8],
        destination: [UInt8],
        sourcePort: Int,
        destinationPort: Int,
        tlvs: [UInt8] = []
    ) -> Phase4ProxyV2Header {
        precondition(source.count == 4 && destination.count == 4)
        let address = source + destination + encodedPort(sourcePort) + encodedPort(destinationPort) + tlvs
        return .init(bytes: signature + [0x21, 0x11] + encodedLength(address.count) + address)
    }

    static func ipv6(
        source: [UInt8],
        destination: [UInt8],
        sourcePort: Int,
        destinationPort: Int,
        tlvs: [UInt8] = []
    ) -> Phase4ProxyV2Header {
        precondition(source.count == 16 && destination.count == 16)
        let address = source + destination + encodedPort(sourcePort) + encodedPort(destinationPort) + tlvs
        return .init(bytes: signature + [0x21, 0x21] + encodedLength(address.count) + address)
    }

    private static func encodedPort(_ port: Int) -> [UInt8] {
        precondition((1 ... 65_535).contains(port))
        return [UInt8((port >> 8) & 0xFF), UInt8(port & 0xFF)]
    }

    private static func encodedLength(_ length: Int) -> [UInt8] {
        [UInt8((length >> 8) & 0xFF), UInt8(length & 0xFF)]
    }
}

final class Phase4ProxyV2Forwarder {
    private let group: EventLoopGroup
    private var channel: Channel?
    private var inputClosedCompletion: Phase2FixtureCompletion<Void>?

    init(group: EventLoopGroup) {
        self.group = group
    }

    var connectedChannel: Channel? { channel }
    var actualPeerAddress: SocketAddress? { channel?.localAddress }
    var inputClosed: EventLoopFuture<Void>? { inputClosedCompletion?.futureResult }

    func connect(listenerPort: Int) throws -> Channel {
        let inputClosedCompletion = Phase2FixtureCompletion<Void>(eventLoop: group.next())
        let channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        Phase4ProxyV2ForwarderCloseHandler(completion: inputClosedCompletion)
                    )
                }
            }
            .connect(host: "127.0.0.1", port: listenerPort))
        self.inputClosedCompletion = inputClosedCompletion
        self.channel = channel
        return channel
    }

    func send(
        header: Phase4ProxyV2Header,
        applicationBytes: [UInt8],
        delivery: Phase4ProxyV2Delivery
    ) throws {
        guard let channel else { throw Phase2FixtureError.closedBeforeExpectedBytes }
        let bytes = header.bytes + applicationBytes
        switch delivery {
        case .coalesced:
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: bytes)))
        case .fragmented(let fragmentSizes):
            try writeFragments(bytes, sizes: fragmentSizes, channel: channel)
        }
    }

    func stop() {
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
    }

    private func writeFragments(_ bytes: [UInt8], sizes: [Int], channel: Channel) throws {
        guard !sizes.isEmpty, sizes.allSatisfy({ $0 > 0 }), sizes.reduce(0, +) == bytes.count else {
            throw Phase2FixtureError.unexpectedBytes
        }
        var offset = 0
        for size in sizes {
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: bytes[offset ..< offset + size])))
            offset += size
        }
    }
}

private final class Phase4ProxyV2ForwarderCloseHandler: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let completion: Phase2FixtureCompletion<Void>

    init(completion: Phase2FixtureCompletion<Void>) {
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.fireChannelRead(data)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            completion.complete(.success(()))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.fireErrorCaught(error)
    }
}

import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix

struct Phase2HTTP2WebSocketExchange: Equatable, Sendable {
    let headers: [(String, String)]
    let data: [UInt8]

    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.headers.elementsEqual(rhs.headers) { $0.0 == $1.0 && $0.1 == $1.1 }
            && lhs.data == rhs.data
    }
}

final class Phase2HTTP2WebSocketPeer {
    static let expectedHeaders = [
        (":method", "CONNECT"),
        (":protocol", "websocket"),
        (":scheme", "https"),
        (":path", "/socket"),
        (":authority", "localhost")
    ]

    private let group: EventLoopGroup
    private let expectedData: [UInt8]
    private let responseData: [UInt8]
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private let completion: Phase2FixtureCompletion<Phase2HTTP2WebSocketExchange>
    private var channel: Channel?

    init(group: EventLoopGroup, expectedData: [UInt8], responseData: [UInt8]) {
        self.group = group
        self.expectedData = expectedData
        self.responseData = responseData
        self.completion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var exchange: EventLoopFuture<Phase2HTTP2WebSocketExchange> { completion.futureResult }

    func start() throws {
        let expectedData = expectedData
        let responseData = responseData
        let completion = completion
        let children = children
        var mutableConnectionConfiguration = NIOHTTP2Handler.ConnectionConfiguration()
        mutableConnectionConfiguration.initialSettings = [HTTP2Setting(parameter: .enableConnectProtocol, value: 1)]
        let connectionConfiguration = mutableConnectionConfiguration
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                children.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return channel.configureHTTP2Pipeline(
                    mode: .server,
                    connectionConfiguration: connectionConfiguration,
                    streamConfiguration: .init()
                ) { stream in
                    stream.eventLoop.makeCompletedFuture {
                        try stream.pipeline.syncOperations.addHandler(Phase2HTTP2WebSocketStreamHandler(
                            expectedData: expectedData,
                            responseData: responseData,
                            completion: completion
                        ))
                    }
                }
                .map { _ in () }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
        try? channel?.close().wait()
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

final class Phase2HTTP2WebSocketStreamHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private static let maximumDataBytes = 64 * 1_024

    private let expectedData: [UInt8]
    private let responseData: [UInt8]
    private let completion: Phase2FixtureCompletion<Phase2HTTP2WebSocketExchange>
    private var headers: [(String, String)]?
    private var data: [UInt8] = []
    private var completed = false

    init(
        expectedData: [UInt8],
        responseData: [UInt8],
        completion: Phase2FixtureCompletion<Phase2HTTP2WebSocketExchange>
    ) {
        self.expectedData = expectedData
        self.responseData = responseData
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .headers(let frame):
            receive(headers: frame.headers, context: context)
        case .data(let frame):
            receive(data: frame.data, context: context)
        default:
            fail(Phase2FixtureError.unexpectedBytes, context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !completed {
            completed = true
            completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error, context: context)
    }

    private func receive(headers: HPACKHeaders, context: ChannelHandlerContext) {
        guard self.headers == nil else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        let pairs = headers.map { ($0.name, $0.value) }
        guard pairs.elementsEqual(Phase2HTTP2WebSocketPeer.expectedHeaders, by: {
            $0.0 == $1.0 && $0.1 == $1.1
        }) else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        self.headers = pairs
        var responseHeaders = HPACKHeaders()
        responseHeaders.add(name: ":status", value: "200")
        context.writeAndFlush(wrapOutboundOut(.headers(.init(headers: responseHeaders))), promise: nil)
    }

    private func receive(data buffer: IOData, context: ChannelHandlerContext) {
        switch buffer {
        case .byteBuffer(let byteBuffer):
            data.append(contentsOf: byteBuffer.readableBytesView)
        case .fileRegion:
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        guard data.count <= Self.maximumDataBytes else {
            fail(Phase2FixtureError.exceededByteLimit, context: context)
            return
        }
        guard data.count >= expectedData.count, let headers else { return }
        guard data == expectedData else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        completed = true
        completion.complete(.success(.init(headers: headers, data: data)))
        let response = ByteBuffer(bytes: responseData)
        let frame = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(response), endStream: false))
        context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        if !completed {
            completed = true
            completion.complete(.failure(error))
        }
        context.close(promise: nil)
    }
}

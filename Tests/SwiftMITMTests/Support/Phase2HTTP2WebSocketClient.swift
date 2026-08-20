import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix

final class Phase2HTTP2WebSocketClient {
    private let group: EventLoopGroup
    private let requestData: [UInt8]
    private let expectedResponseData: [UInt8]
    private let responseCompletion: Phase2FixtureCompletion<[UInt8]>
    private var connection: Channel?
    private var stream: Channel?

    init(group: EventLoopGroup, requestData: [UInt8], expectedResponseData: [UInt8]) {
        self.group = group
        self.requestData = requestData
        self.expectedResponseData = expectedResponseData
        self.responseCompletion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var response: EventLoopFuture<[UInt8]> { responseCompletion.futureResult }

    func connectAndExchange(port: Int) throws {
        let settingsCompletion = Phase2FixtureCompletion<Void>(eventLoop: group.next())
        let multiplexerCompletion = Phase2FixtureCompletion<NIOHTTP2Handler.StreamMultiplexer>(eventLoop: group.next())
        let connection = try ClientBootstrap(group: group)
            .channelInitializer { channel in
                let gateConfigured = channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        Phase2HTTP2SettingsGate(completion: settingsCompletion)
                    )
                }
                return gateConfigured.flatMap {
                    channel.configureHTTP2Pipeline(
                        mode: .client,
                        connectionConfiguration: .init(),
                        streamConfiguration: .init()
                    ) { $0.close() }
                }
                .map { multiplexer in
                    multiplexerCompletion.complete(.success(multiplexer))
                }
            }
            .connect(host: "127.0.0.1", port: port)
            .wait()
        self.connection = connection
        try settingsCompletion.futureResult.wait()
        let multiplexer = try multiplexerCompletion.futureResult.wait()

        let requestData = requestData
        let expectedResponseData = expectedResponseData
        let responseCompletion = responseCompletion
        stream = try multiplexer.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase2HTTP2WebSocketClientStreamHandler(
                    requestData: requestData,
                    expectedResponseData: expectedResponseData,
                    responseCompletion: responseCompletion
                ))
            }
        }
        .wait()
    }

    func stop() {
        try? stream?.close().wait()
        try? connection?.close().wait()
        responseCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase2HTTP2SettingsGate: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private static let maximumSettingsBytes = 1_024

    private let completion: Phase2FixtureCompletion<Void>
    private var bytes: [UInt8] = []
    private var completed = false

    init(completion: Phase2FixtureCompletion<Void>) {
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        if !completed {
            bytes.append(contentsOf: buffer.readableBytesView)
            inspectSettings(context: context)
        }
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        complete(.failure(error))
        context.fireErrorCaught(error)
    }

    private func inspectSettings(context: ChannelHandlerContext) {
        guard bytes.count <= Self.maximumSettingsBytes else {
            complete(.failure(Phase2FixtureError.exceededByteLimit))
            context.close(promise: nil)
            return
        }
        guard bytes.count >= 9 else { return }
        let length = Int(bytes[0]) << 16 | Int(bytes[1]) << 8 | Int(bytes[2])
        guard bytes.count >= 9 + length else { return }
        guard bytes[3] == 0x04, bytes[5 ... 8].allSatisfy({ $0 == 0 }), length.isMultiple(of: 6) else {
            complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        var offset = 9
        while offset < 9 + length {
            let identifier = UInt16(bytes[offset]) << 8 | UInt16(bytes[offset + 1])
            let value = UInt32(bytes[offset + 2]) << 24
                | UInt32(bytes[offset + 3]) << 16
                | UInt32(bytes[offset + 4]) << 8
                | UInt32(bytes[offset + 5])
            if identifier == 0x08, value == 1 {
                complete(.success(()))
                return
            }
            offset += 6
        }
        complete(.failure(Phase2FixtureError.unexpectedBytes))
        context.close(promise: nil)
    }

    private func complete(_ result: Result<Void, Error>) {
        guard !completed else { return }
        completed = true
        completion.complete(result)
    }
}

private final class Phase2HTTP2WebSocketClientStreamHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let requestData: [UInt8]
    private let expectedResponseData: [UInt8]
    private let responseCompletion: Phase2FixtureCompletion<[UInt8]>
    private var accepted = false
    private var bytes: [UInt8] = []

    init(
        requestData: [UInt8],
        expectedResponseData: [UInt8],
        responseCompletion: Phase2FixtureCompletion<[UInt8]>
    ) {
        self.requestData = requestData
        self.expectedResponseData = expectedResponseData
        self.responseCompletion = responseCompletion
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        Phase2HTTP2WebSocketPeer.expectedHeaders.forEach { headers.add(name: $0.0, value: $0.1) }
        context.write(wrapOutboundOut(.headers(.init(headers: headers))), promise: nil)
        let data = ByteBuffer(bytes: requestData)
        context.writeAndFlush(wrapOutboundOut(.data(.init(data: .byteBuffer(data), endStream: false))), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .headers(let frame):
            accepted = frame.headers.first(name: ":status") == "200"
        case .data(let frame):
            receive(frame.data, context: context)
        default:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        responseCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        responseCompletion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func receive(_ data: IOData, context: ChannelHandlerContext) {
        guard accepted else {
            responseCompletion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        guard case .byteBuffer(let buffer) = data else {
            responseCompletion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        bytes.append(contentsOf: buffer.readableBytesView)
        guard bytes.count >= expectedResponseData.count else { return }
        let result: Result<[UInt8], Error> = bytes == expectedResponseData
            ? .success(bytes)
            : .failure(Phase2FixtureError.unexpectedBytes)
        responseCompletion.complete(result)
    }
}

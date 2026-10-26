import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

enum Phase3HTTP2WebSocketOriginScenario: Sendable {
    case accepted
    case finalStatus(Int)
    case prematureSuccessEnd
    case reset
    case connectionLoss
}

struct Phase3HTTP2WebSocketOriginResult: Sendable {
    let headers: [(String, String)]
    let clientBytes: [UInt8]
}

final class Phase3TLSHTTP2WebSocketOrigin: @unchecked Sendable {
    let caCertificatePEM: String
    let hostname = "localhost"

    private let group: EventLoopGroup
    private let sslContext: NIOSSLContext
    private let advertisesExtendedConnect: Bool
    private let scenario: Phase3HTTP2WebSocketOriginScenario
    private let completion: Phase2FixtureCompletion<Phase3HTTP2WebSocketOriginResult>
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(
        group: EventLoopGroup,
        advertisesExtendedConnect: Bool = true,
        scenario: Phase3HTTP2WebSocketOriginScenario = .accepted
    ) throws {
        self.group = group
        self.advertisesExtendedConnect = advertisesExtendedConnect
        self.scenario = scenario
        let ca = try CertificateAuthority(commonName: "SwiftMITM Phase 3 Origin Root")
        caCertificatePEM = ca.caCertificatePEM
        let leaf = try ca.leaf(forHost: hostname)
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: leaf.certificateChain,
            privateKey: leaf.privateKey
        )
        configuration.applicationProtocols = ["h2"]
        sslContext = try NIOSSLContext(configuration: configuration)
        completion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var result: EventLoopFuture<Phase3HTTP2WebSocketOriginResult> { completion.futureResult }

    func start() throws {
        let sslContext = sslContext
        let advertisesExtendedConnect = advertisesExtendedConnect
        let scenario = scenario
        let completion = completion
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
                    try channel.pipeline.syncOperations.addHandlers([
                        NIOSSLServerHandler(context: sslContext),
                        ApplicationProtocolNegotiationHandler { result, channel in
                            Self.configureHTTP2(
                                result: result,
                                channel: channel,
                                advertisesExtendedConnect: advertisesExtendedConnect,
                                scenario: scenario,
                                completion: completion
                            )
                        }
                    ])
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
        try? channel?.close().wait()
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }

    private static func configureHTTP2(
        result: ALPNResult,
        channel: Channel,
        advertisesExtendedConnect: Bool,
        scenario: Phase3HTTP2WebSocketOriginScenario,
        completion: Phase2FixtureCompletion<Phase3HTTP2WebSocketOriginResult>
    ) -> EventLoopFuture<Void> {
        guard case .negotiated("h2") = result else {
            return channel.eventLoop.makeFailedFuture(Phase2FixtureError.unexpectedBytes)
        }
        var mutableConfiguration = NIOHTTP2Handler.ConnectionConfiguration()
        if advertisesExtendedConnect {
            mutableConfiguration.initialSettings = [HTTP2Setting(parameter: .enableConnectProtocol, value: 1)]
        }
        let configuration = mutableConfiguration
        return channel.configureHTTP2Pipeline(
            mode: .server,
            connectionConfiguration: configuration,
            streamConfiguration: .init()
        ) { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase3HTTP2WebSocketOriginStreamHandler(
                    scenario: scenario,
                    completion: completion
                ))
            }
        }
        .map { _ in () }
    }
}

private final class Phase3HTTP2WebSocketOriginStreamHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let scenario: Phase3HTTP2WebSocketOriginScenario
    private let completion: Phase2FixtureCompletion<Phase3HTTP2WebSocketOriginResult>
    private var headers: [(String, String)]?
    private var clientBytes: [UInt8] = []
    private var sentBinary = false
    private var completed = false

    init(
        scenario: Phase3HTTP2WebSocketOriginScenario,
        completion: Phase2FixtureCompletion<Phase3HTTP2WebSocketOriginResult>
    ) {
        self.scenario = scenario
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .headers(let frame):
            receive(headers: frame.headers, context: context)
        case .data(let frame):
            receive(data: frame.data, context: context)
        default:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !completed {
            completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func receive(headers: HPACKHeaders, context: ChannelHandlerContext) {
        let pairs = headers.map { ($0.name, $0.value) }
        guard isValidExtendedConnect(pairs), self.headers == nil else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        self.headers = pairs
        switch scenario {
        case .accepted:
            sendStatus(200, endStream: false, context: context)
        case .finalStatus(let status):
            sendStatus(status, endStream: true, context: context)
            complete()
        case .prematureSuccessEnd:
            sendStatus(200, endStream: true, context: context)
            complete()
        case .reset, .connectionLoss:
            sendStatus(200, endStream: false, context: context)
        }
    }

    private func receive(data: IOData, context: ChannelHandlerContext) {
        guard case .byteBuffer(let buffer) = data else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        clientBytes.append(contentsOf: buffer.readableBytesView)
        switch scenario {
        case .reset:
            complete()
            context.close(promise: nil)
            return
        case .connectionLoss:
            complete()
            context.channel.parent?.close(promise: nil)
            return
        case .accepted:
            break
        case .finalStatus, .prematureSuccessEnd:
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        let expected = WebSocketWire.clientFrames
        guard
            clientBytes.count <= expected.count,
            clientBytes == Array(expected.prefix(clientBytes.count))
        else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        if clientBytes.count >= WebSocketWire.clientTextFrame.count, !sentBinary {
            sentBinary = true
            send(WebSocketWire.serverBinaryFrame, context: context)
        }
        guard clientBytes.count == expected.count else { return }
        send(WebSocketWire.serverCloseFrame, context: context)
        complete()
    }

    private func isValidExtendedConnect(_ headers: [(String, String)]) -> Bool {
        let expected = [
            ":method": "CONNECT",
            ":protocol": "websocket",
            ":scheme": "https",
            ":path": "/socket"
        ]
        let fields = Dictionary(uniqueKeysWithValues: headers)
        return expected.allSatisfy { fields[$0.key] == $0.value }
            && fields[":authority"] != nil
    }

    private func sendStatus(_ status: Int, endStream: Bool, context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: String(status))
        let frame = HTTP2Frame.FramePayload.headers(.init(headers: headers, endStream: endStream))
        context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
    }

    private func send(_ bytes: [UInt8], context: ChannelHandlerContext) {
        let buffer = ByteBuffer(bytes: bytes)
        let frame = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(buffer), endStream: false))
        context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
    }

    private func complete() {
        guard !completed, let headers else { return }
        completed = true
        completion.complete(.success(.init(headers: headers, clientBytes: clientBytes)))
    }
}

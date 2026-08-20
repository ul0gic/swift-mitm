import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

struct Phase3ConcurrentOriginResult: Sendable {
    let ordinaryRequestCount: Int
    let socketABytes: [UInt8]
    let socketBBytes: [UInt8]
    let socketBTerminatedAfterViolation: Bool
}

final class Phase3ConcurrentHTTP2WebSocketOrigin: @unchecked Sendable {
    let caCertificatePEM: String
    let hostname = "localhost"

    private let group: EventLoopGroup
    private let sslContext: NIOSSLContext
    private let recorder: Phase3ConcurrentOriginRecorder
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup) throws {
        self.group = group
        let ca = try CertificateAuthority(commonName: "SwiftMITM Phase 3 Concurrent Root")
        caCertificatePEM = ca.caCertificatePEM
        let leaf = try ca.leaf(forHost: hostname)
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: leaf.certificateChain,
            privateKey: leaf.privateKey
        )
        configuration.applicationProtocols = ["h2"]
        sslContext = try NIOSSLContext(configuration: configuration)
        recorder = Phase3ConcurrentOriginRecorder(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var result: EventLoopFuture<Phase3ConcurrentOriginResult> { recorder.futureResult }

    func start() throws {
        let sslContext = sslContext
        let recorder = recorder
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
                            Self.configureHTTP2(result: result, channel: channel, recorder: recorder)
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
        recorder.failIfPending()
    }

    private static func configureHTTP2(
        result: ALPNResult,
        channel: Channel,
        recorder: Phase3ConcurrentOriginRecorder
    ) -> EventLoopFuture<Void> {
        guard case .negotiated("h2") = result else {
            return channel.eventLoop.makeFailedFuture(Phase2FixtureError.unexpectedBytes)
        }
        var mutableConfiguration = NIOHTTP2Handler.ConnectionConfiguration()
        mutableConfiguration.initialSettings = [HTTP2Setting(parameter: .enableConnectProtocol, value: 1)]
        let configuration = mutableConfiguration
        return channel.configureHTTP2Pipeline(
            mode: .server,
            connectionConfiguration: configuration,
            streamConfiguration: .init()
        ) { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(
                    Phase3ConcurrentOriginStreamHandler(recorder: recorder)
                )
            }
        }
        .map { _ in () }
    }
}

private final class Phase3ConcurrentOriginRecorder: @unchecked Sendable {
    let futureResult: EventLoopFuture<Phase3ConcurrentOriginResult>

    private struct State {
        var ordinaryRequestCount = 0
        var socketABytes: [UInt8]?
        var socketBBytes: [UInt8]?
        var socketBTerminatedAfterViolation = false
    }

    private let state = NIOLockedValueBox(State())
    private let completion: Phase2FixtureCompletion<Phase3ConcurrentOriginResult>

    init(eventLoop: EventLoop) {
        completion = Phase2FixtureCompletion(eventLoop: eventLoop)
        futureResult = completion.futureResult
    }

    func recordOrdinaryRequest() {
        update { $0.ordinaryRequestCount += 1 }
    }

    func recordWebSocket(path: String, bytes: [UInt8]) {
        update { state in
            if path == "/socket/a" {
                state.socketABytes = bytes
            } else if path == "/socket/b" {
                state.socketBBytes = bytes
            }
        }
    }

    func recordSocketBTermination() {
        update { $0.socketBTerminatedAfterViolation = true }
    }

    func failIfPending() {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }

    private func update(_ body: (inout State) -> Void) {
        let result = state.withLockedValue { state -> Phase3ConcurrentOriginResult? in
            body(&state)
            guard
                state.ordinaryRequestCount == 1,
                let socketABytes = state.socketABytes,
                let socketBBytes = state.socketBBytes,
                state.socketBTerminatedAfterViolation
            else { return nil }
            return .init(
                ordinaryRequestCount: state.ordinaryRequestCount,
                socketABytes: socketABytes,
                socketBBytes: socketBBytes,
                socketBTerminatedAfterViolation: state.socketBTerminatedAfterViolation
            )
        }
        if let result {
            completion.complete(.success(result))
        }
    }
}

private final class Phase3ConcurrentOriginStreamHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let recorder: Phase3ConcurrentOriginRecorder
    private var path: String?
    private var bytes: [UInt8] = []
    private var sentBinary = false
    private var sentViolation = false

    init(recorder: Phase3ConcurrentOriginRecorder) {
        self.recorder = recorder
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

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        if path == "/socket/b", sentViolation {
            recorder.recordSocketBTermination()
        }
        context.fireChannelInactive()
    }

    private func receive(headers: HPACKHeaders, context: ChannelHandlerContext) {
        guard path == nil, let path = headers.first(name: ":path") else {
            context.close(promise: nil)
            return
        }
        self.path = path
        if path == "/ordinary" {
            recorder.recordOrdinaryRequest()
            sendStatus(context: context)
            let response = ByteBuffer(bytes: Array("ordinary-response".utf8))
            let data = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(response), endStream: true))
            context.writeAndFlush(wrapOutboundOut(data), promise: nil)
            return
        }
        guard path == "/socket/a" || path == "/socket/b" else {
            context.close(promise: nil)
            return
        }
        sendStatus(context: context)
    }

    private func receive(data: IOData, context: ChannelHandlerContext) {
        guard let path, case .byteBuffer(let buffer) = data else {
            context.close(promise: nil)
            return
        }
        bytes.append(contentsOf: buffer.readableBytesView)
        if path == "/socket/b" {
            guard bytes == WebSocketWire.clientTextFrame else {
                context.close(promise: nil)
                return
            }
            recorder.recordWebSocket(path: path, bytes: bytes)
            sentViolation = true
            send([0xA2, 0x00], context: context)
            return
        }
        let expected = WebSocketWire.clientFrames
        guard bytes.count <= expected.count, bytes == Array(expected.prefix(bytes.count)) else {
            context.close(promise: nil)
            return
        }
        if bytes.count >= WebSocketWire.clientTextFrame.count, !sentBinary {
            sentBinary = true
            send(WebSocketWire.serverBinaryFrame, context: context)
        }
        guard bytes.count == expected.count else { return }
        recorder.recordWebSocket(path: path, bytes: bytes)
        send(WebSocketWire.serverCloseFrame, context: context)
    }

    private func sendStatus(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: "200")
        context.write(wrapOutboundOut(.headers(.init(headers: headers))), promise: nil)
        context.flush()
    }

    private func send(_ bytes: [UInt8], context: ChannelHandlerContext) {
        let buffer = ByteBuffer(bytes: bytes)
        let data = HTTP2Frame.FramePayload.data(.init(data: .byteBuffer(buffer), endStream: false))
        context.writeAndFlush(wrapOutboundOut(data), promise: nil)
    }
}

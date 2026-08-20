import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

struct Phase4OpaqueScenario: Sendable {
    let name: String
    let clientBytes: [UInt8]
    let serverBytes: [UInt8]
    let serverReplyBytes: [UInt8]
    let clientClosesOutput: Bool
    let serverClosesOutput: Bool
    let clientClosesAbruptly: Bool
    let clientStartsStalled: Bool
}

enum Phase4OpaqueScenarios {
    static let clientFirst = converted(Phase2OpaqueTCPScenarios.clientFirst)
    static let serverFirst = converted(Phase2OpaqueTCPScenarios.serverFirst)
    static let bidirectional = converted(Phase2OpaqueTCPScenarios.bidirectional)
    static let clientHalfClose = Phase4OpaqueScenario(
        name: Phase2OpaqueTCPScenarios.clientHalfClose.name,
        clientBytes: Phase2OpaqueTCPScenarios.clientHalfClose.clientInitialBytes,
        serverBytes: [],
        serverReplyBytes: Phase2OpaqueTCPScenarios.clientHalfClose.serverReplyBytes,
        clientClosesOutput: true,
        serverClosesOutput: false,
        clientClosesAbruptly: false,
        clientStartsStalled: false
    )
    static let serverHalfClose = Phase4OpaqueScenario(
        name: Phase2OpaqueTCPScenarios.serverHalfClose.name,
        clientBytes: Phase2OpaqueTCPScenarios.serverHalfClose.clientInitialBytes,
        serverBytes: Phase2OpaqueTCPScenarios.serverHalfClose.serverInitialBytes,
        serverReplyBytes: [],
        clientClosesOutput: false,
        serverClosesOutput: true,
        clientClosesAbruptly: false,
        clientStartsStalled: false
    )
    static let zeroByte = Phase4OpaqueScenario(
        name: "zero-byte",
        clientBytes: [],
        serverBytes: [],
        serverReplyBytes: [],
        clientClosesOutput: true,
        serverClosesOutput: true,
        clientClosesAbruptly: false,
        clientStartsStalled: false
    )
    static let abrupt = Phase4OpaqueScenario(
        name: "abrupt-client-close",
        clientBytes: [0xDE, 0xAD, 0xBE, 0xEF],
        serverBytes: [],
        serverReplyBytes: [],
        clientClosesOutput: false,
        serverClosesOutput: false,
        clientClosesAbruptly: true,
        clientStartsStalled: false
    )
    static let stalled = Phase4OpaqueScenario(
        name: Phase2OpaqueTCPScenarios.stalledReader.name,
        clientBytes: [],
        serverBytes: Phase2OpaqueTCPScenarios.stalledReader.serverInitialBytes,
        serverReplyBytes: [],
        clientClosesOutput: false,
        serverClosesOutput: false,
        clientClosesAbruptly: false,
        clientStartsStalled: true
    )

    static let all = [
        clientFirst,
        serverFirst,
        bidirectional,
        clientHalfClose,
        serverHalfClose,
        zeroByte,
        abrupt,
        stalled
    ]

    private static func converted(_ scenario: Phase2OpaqueTCPScenario) -> Phase4OpaqueScenario {
        .init(
            name: scenario.name,
            clientBytes: scenario.clientInitialBytes,
            serverBytes: scenario.serverInitialBytes,
            serverReplyBytes: scenario.serverReplyBytes,
            clientClosesOutput: false,
            serverClosesOutput: false,
            clientClosesAbruptly: false,
            clientStartsStalled: false
        )
    }
}

final class Phase4OpaquePeer {
    let terminal = Phase4TerminalObservation()

    private let group: EventLoopGroup
    private let scenario: Phase4OpaqueScenario
    private let bindHost: String
    private let acceptedCompletion: Phase2FixtureCompletion<Void>
    private let receivedCompletion: Phase2FixtureCompletion<[UInt8]>
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup, scenario: Phase4OpaqueScenario, bindHost: String = "127.0.0.1") {
        self.group = group
        self.scenario = scenario
        self.bindHost = bindHost
        acceptedCompletion = Phase2FixtureCompletion(eventLoop: group.next())
        receivedCompletion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var accepted: EventLoopFuture<Void> { acceptedCompletion.futureResult }
    var receivedBytes: EventLoopFuture<[UInt8]> { receivedCompletion.futureResult }

    func start() throws {
        let scenario = scenario
        let terminal = terminal
        let acceptedCompletion = acceptedCompletion
        let receivedCompletion = receivedCompletion
        let children = children
        let bindHost = bindHost
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
                    try channel.pipeline.syncOperations.addHandler(Phase4OpaquePeerHandler(
                        scenario: scenario,
                        terminal: terminal,
                        acceptedCompletion: acceptedCompletion,
                        receivedCompletion: receivedCompletion
                    ))
                }
            }
            .bind(host: bindHost, port: 0))
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? phase4BoundedWait($0.close()) }
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        acceptedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        receivedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase4OpaquePeerHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let scenario: Phase4OpaqueScenario
    private let terminal: Phase4TerminalObservation
    private let acceptedCompletion: Phase2FixtureCompletion<Void>
    private let receivedCompletion: Phase2FixtureCompletion<[UInt8]>
    private var bytes: [UInt8] = []
    private var completed = false

    init(
        scenario: Phase4OpaqueScenario,
        terminal: Phase4TerminalObservation,
        acceptedCompletion: Phase2FixtureCompletion<Void>,
        receivedCompletion: Phase2FixtureCompletion<[UInt8]>
    ) {
        self.scenario = scenario
        self.terminal = terminal
        self.acceptedCompletion = acceptedCompletion
        self.receivedCompletion = receivedCompletion
    }

    func channelActive(context: ChannelHandlerContext) {
        acceptedCompletion.complete(.success(()))
        if scenario.clientBytes.isEmpty {
            complete([])
        }
        guard !scenario.serverBytes.isEmpty else { return }
        let write = context.writeAndFlush(NIOAny(ByteBuffer(bytes: scenario.serverBytes)))
        if scenario.serverClosesOutput {
            let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
            write.whenComplete { _ in boundContext.value.close(mode: .output, promise: nil) }
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        bytes.append(contentsOf: unwrapInboundIn(data).readableBytesView)
        guard bytes.count <= Phase2OpaqueTCPScenario.maximumPayloadBytes else {
            fail(Phase2FixtureError.exceededByteLimit, context: context)
            return
        }
        guard bytes.count >= scenario.clientBytes.count else { return }
        guard bytes == scenario.clientBytes else {
            fail(Phase2FixtureError.unexpectedBytes, context: context)
            return
        }
        complete(bytes)
        if !scenario.serverReplyBytes.isEmpty {
            context.writeAndFlush(NIOAny(ByteBuffer(bytes: scenario.serverReplyBytes)), promise: nil)
        }
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            terminal.recordInputClosed()
            if scenario.clientBytes.isEmpty {
                complete([])
            }
            if scenario.serverClosesOutput, scenario.serverBytes.isEmpty {
                context.close(mode: .output, promise: nil)
            }
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        terminal.recordInactive()
        if !completed, bytes == scenario.clientBytes {
            complete(bytes)
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        terminal.recordError()
        fail(error, context: context)
    }

    private func complete(_ bytes: [UInt8]) {
        guard !completed else { return }
        completed = true
        receivedCompletion.complete(.success(bytes))
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        if !completed {
            completed = true
            receivedCompletion.complete(.failure(error))
        }
        context.close(promise: nil)
    }
}

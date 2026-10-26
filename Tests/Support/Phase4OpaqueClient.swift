import NIOCore
import NIOPosix

final class Phase4OpaqueClient {
    let terminal = Phase4TerminalObservation()

    private let group: EventLoopGroup
    private var channel: Channel?
    private var inboundObservation: Phase4ExactBytes?

    init(group: EventLoopGroup) {
        self.group = group
    }

    var inboundBytes: EventLoopFuture<[UInt8]>? { inboundObservation?.futureResult }
    var observedInboundBytes: [UInt8] { inboundObservation?.observedBytes ?? [] }
    var isActive: Bool { channel?.isActive == true }

    func connect(port: Int, scenario: Phase4OpaqueScenario) throws {
        let expected = scenario.serverBytes + scenario.serverReplyBytes
        let observation = Phase4ExactBytes(
            eventLoop: group.next(),
            expected: expected,
            maximumBytes: Phase2OpaqueTCPScenario.maximumPayloadBytes
        )
        let terminal = terminal
        inboundObservation = observation
        channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .channelOption(ChannelOptions.autoRead, value: !scenario.clientStartsStalled)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase4OpaqueClientHandler(
                        terminal: terminal,
                        observation: observation
                    ))
                }
            }
            .connect(host: "127.0.0.1", port: port))
        if !scenario.clientBytes.isEmpty {
            if let channel {
                try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: scenario.clientBytes)))
            }
        }
        if scenario.clientClosesOutput {
            if let channel {
                try phase4BoundedWait(channel.close(mode: .output))
            }
        } else if scenario.clientClosesAbruptly {
            if let channel {
                try phase4BoundedWait(channel.close())
            }
        }
    }

    func resumeReads() throws {
        guard let channel else { throw Phase2FixtureError.closedBeforeExpectedBytes }
        try phase4BoundedWait(channel.setOption(ChannelOptions.autoRead, value: true))
        channel.read()
    }

    func stop() {
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        inboundObservation?.close()
    }
}

private final class Phase4OpaqueClientHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let terminal: Phase4TerminalObservation
    private let observation: Phase4ExactBytes

    init(
        terminal: Phase4TerminalObservation,
        observation: Phase4ExactBytes
    ) {
        self.terminal = terminal
        self.observation = observation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        observation.append(unwrapInboundIn(data).readableBytesView)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            terminal.recordInputClosed()
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        terminal.recordInactive()
        observation.close()
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        terminal.recordError()
        observation.close()
        context.close(promise: nil)
    }
}

import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Phase4PublicOpaqueTests: XCTestCase {
    func testOpaqueScenarioMatrixForwardsExactBytesWithIndependentCaptureBounds() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink)

        do {
            let proxyPort = try await proxy.start(port: 0)
            for (index, scenario) in Phase4OpaqueScenarios.all.enumerated() {
                try runScenario(
                    scenario,
                    proxyPort: proxyPort,
                    expectedFlowCount: index + 1,
                    group: group,
                    sink: sink
                )
            }
            try await proxy.stop()
            try await group.shutdownGracefully()
        } catch {
            try? await proxy.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testOpaqueTwoSidedHalfCloseEmitsOneDirectionEndPerSideAndOneClose() async throws {
        let scenario = makeTwoSidedHalfCloseScenario()
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink)
        let peer = Phase4OpaquePeer(group: group, scenario: scenario)
        let client = Phase4TransparentOpaqueClient(group: group)

        do {
            try peer.start()
            let proxyPort = try await proxy.start(port: 0)
            try client.connect(proxyPort: proxyPort, originPort: peer.localPort, scenario: scenario)
            try await peer.accepted.get()
            try client.applyClientTerminal(scenario)
            let peerBytes = try await peer.receivedBytes.get()
            let clientInbound = try XCTUnwrap(client.inboundBytes)
            let clientBytes = try await clientInbound.get()
            XCTAssertEqual(peerBytes, scenario.clientBytes)
            XCTAssertEqual(clientBytes, scenario.serverBytes)
            let snapshot: Phase4CaptureSink.Snapshot
            do {
                snapshot = try sink.wait {
                    $0.opaqueDirectionEnds.count == 2 && $0.opaqueCloses.count == 1
                }
            } catch {
                let current = sink.snapshot
                XCTFail(
                    "terminal timeout ends=\(current.opaqueDirectionEnds) "
                        + "closes=\(current.opaqueCloses) errors=\(current.opaqueErrors)"
                )
                throw error
            }

            let flowID = try XCTUnwrap(snapshot.opaqueFlows.first?.id)
            XCTAssertEqual(snapshot.opaqueDirectionEnds.map(\.flowID), [flowID, flowID])
            XCTAssertEqual(Set(snapshot.opaqueDirectionEnds.map(\.direction)), [
                .clientToServer, .serverToClient
            ])
            XCTAssertEqual(snapshot.opaqueCloses, [.init(flowID: flowID, reason: .completed)])
            XCTAssertTrue(snapshot.opaqueErrors.isEmpty)
            try await finish(proxy: proxy, peer: peer, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
            throw error
        }
    }

    func testTrustedIPv6DestinationRoutesToIPv6OriginWithExactOpaqueCapture() async throws {
        let scenario = makeIPv6Scenario()
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink)
        let peer = Phase4OpaquePeer(group: group, scenario: scenario, bindHost: "::1")
        let client = Phase4TransparentOpaqueClient(group: group)

        do {
            try peer.start()
            let proxyPort = try await proxy.start(port: 0)
            try client.connect(
                proxyPort: proxyPort,
                scenario: scenario,
                header: ipv6IngressHeader(destinationPort: peer.localPort)
            )
            let peerBytes = try await peer.receivedBytes.get()
            let clientInbound = try XCTUnwrap(client.inboundBytes)
            let clientBytes = try await clientInbound.get()
            let snapshot = try sink.wait { $0.opaqueData.reduce(0) { $0 + $1.byteCount } == 4 }

            XCTAssertEqual(peerBytes, scenario.clientBytes)
            XCTAssertEqual(clientBytes, scenario.serverBytes)
            let flow = try XCTUnwrap(snapshot.opaqueFlows.first)
            XCTAssertEqual(flow.target.destination.address, "::1")
            XCTAssertEqual(flow.target.destination.port, peer.localPort)
            XCTAssertEqual(flow.target.logicalAuthority, "[::1]:\(peer.localPort)")
            XCTAssertNil(flow.target.tlsServerName)
            XCTAssertEqual(flow.target.ingressProvenance, .trustedProxyV2)
            XCTAssertEqual(flow.target.originalClient?.address, "2001:db8::20")
            XCTAssertEqual(flow.target.originalClient?.port, 50_004)
            assertCaptureBound(snapshot, flowID: flow.id, scenario: scenario)
            try await finish(proxy: proxy, peer: peer, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
            throw error
        }
    }

    private func makeTwoSidedHalfCloseScenario() -> Phase4OpaqueScenario {
        Phase4OpaqueScenario(
            name: "two-sided-half-close",
            clientBytes: [0x43, 0x48],
            serverBytes: [0x53, 0x48],
            serverReplyBytes: [],
            clientClosesOutput: true,
            serverClosesOutput: true,
            clientClosesAbruptly: false,
            clientStartsStalled: false
        )
    }

    private func makeIPv6Scenario() -> Phase4OpaqueScenario {
        Phase4OpaqueScenario(
            name: "ipv6-destination",
            clientBytes: [0x49, 0x36],
            serverBytes: [0x56, 0x36],
            serverReplyBytes: [],
            clientClosesOutput: false,
            serverClosesOutput: false,
            clientClosesAbruptly: false,
            clientStartsStalled: false
        )
    }

    private func ipv6IngressHeader(destinationPort: Int) -> Phase4ProxyV2Header {
        .ipv6(
            source: [0x20, 0x01, 0x0D, 0xB8] + Array(repeating: 0, count: 11) + [0x20],
            destination: Array(repeating: 0, count: 15) + [0x01],
            sourcePort: 50_004,
            destinationPort: destinationPort
        )
    }

    private func runScenario(
        _ scenario: Phase4OpaqueScenario,
        proxyPort: Int,
        expectedFlowCount: Int,
        group: EventLoopGroup,
        sink: Phase4CaptureSink
    ) throws {
        let peer = Phase4OpaquePeer(group: group, scenario: scenario)
        let client = Phase4TransparentOpaqueClient(group: group)
        defer {
            client.stop()
            peer.stop()
        }

        try peer.start()
        try client.connect(proxyPort: proxyPort, originPort: peer.localPort, scenario: scenario)
        try peer.accepted.wait()
        try client.applyClientTerminal(scenario)
        if scenario.clientStartsStalled {
            XCTAssertEqual(client.observedInboundBytes, [], scenario.name)
            try client.resumeReads()
        }
        XCTAssertEqual(try peer.receivedBytes.wait(), scenario.clientBytes, scenario.name)
        XCTAssertEqual(
            try client.inboundBytes?.wait(),
            scenario.serverBytes + scenario.serverReplyBytes,
            scenario.name
        )
        let snapshot = try sink.wait { $0.opaqueFlows.count >= expectedFlowCount }
        let flow = try XCTUnwrap(snapshot.opaqueFlows.last)
        assertTarget(flow.target, originPort: peer.localPort, scenario: scenario.name)
        assertCaptureBound(snapshot, flowID: flow.id, scenario: scenario)
    }

    private func assertCaptureBound(
        _ snapshot: Phase4CaptureSink.Snapshot,
        flowID: UUID,
        scenario: Phase4OpaqueScenario
    ) {
        let data = snapshot.opaqueData.filter { $0.flowID == flowID }
        assertDirectionCapture(data, direction: .clientToServer, expected: scenario.clientBytes)
        assertDirectionCapture(
            data,
            direction: .serverToClient,
            expected: scenario.serverBytes + scenario.serverReplyBytes
        )
    }

    private func assertDirectionCapture(
        _ data: [Phase4CaptureSink.OpaqueData],
        direction: OpaqueFlowDirection,
        expected: [UInt8]
    ) {
        let directional = data.filter { $0.direction == direction }
        XCTAssertEqual(directional.reduce(0) { $0 + $1.byteCount }, expected.count)
        XCTAssertEqual(directional.flatMap(\.bytes), Array(expected.prefix(2)))
    }

    private func assertTarget(_ target: CapturedTarget, originPort: Int, scenario: String) {
        XCTAssertEqual(target.destination.address, "127.0.0.1", scenario)
        XCTAssertEqual(target.destination.port, originPort, scenario)
        XCTAssertEqual(target.logicalAuthority, "127.0.0.1:\(originPort)", scenario)
        XCTAssertNil(target.tlsServerName, scenario)
        XCTAssertEqual(target.ingressProvenance, .trustedProxyV2, scenario)
        XCTAssertEqual(target.originalClient?.address, "192.0.2.20", scenario)
        XCTAssertEqual(target.originalClient?.port, 50_001, scenario)
    }

    private func makeProxy(group: EventLoopGroup, sink: Phase4CaptureSink) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            classificationDeadline: .milliseconds(20)
        ))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            opaqueCaptureByteLimit: 2
        )
    }

    private func finish(
        proxy: ProxyServer,
        peer: Phase4OpaquePeer,
        client: Phase4TransparentOpaqueClient,
        group: EventLoopGroup
    ) async throws {
        client.stop()
        try await proxy.stop()
        peer.stop()
        try await group.shutdownGracefully()
    }

    private func cleanup(
        proxy: ProxyServer,
        peer: Phase4OpaquePeer,
        client: Phase4TransparentOpaqueClient,
        group: EventLoopGroup
    ) async {
        client.stop()
        try? await proxy.stop()
        peer.stop()
        try? await group.shutdownGracefully()
    }
}

private final class Phase4TransparentOpaqueClient {
    let terminal = Phase4TerminalObservation()

    private let group: EventLoopGroup
    private var channel: Channel?
    private var observation: Phase4ExactBytes?

    init(group: EventLoopGroup) {
        self.group = group
    }

    var inboundBytes: EventLoopFuture<[UInt8]>? { observation?.futureResult }
    var observedInboundBytes: [UInt8] { observation?.observedBytes ?? [] }

    func connect(
        proxyPort: Int,
        originPort: Int = 0,
        scenario: Phase4OpaqueScenario,
        header: Phase4ProxyV2Header? = nil
    ) throws {
        let expected = scenario.serverBytes + scenario.serverReplyBytes
        let observation = Phase4ExactBytes(
            eventLoop: group.next(),
            expected: expected,
            maximumBytes: Phase2OpaqueTCPScenario.maximumPayloadBytes
        )
        let terminal = terminal
        self.observation = observation
        let channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .channelOption(ChannelOptions.autoRead, value: !scenario.clientStartsStalled)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase4TransparentOpaqueClientHandler(
                        terminal: terminal,
                        observation: observation
                    ))
                }
            }
            .connect(host: "127.0.0.1", port: proxyPort))
        self.channel = channel
        let ingressHeader = header ?? Phase4ProxyV2Header.ipv4(
            source: [192, 0, 2, 20],
            destination: [127, 0, 0, 1],
            sourcePort: 50_001,
            destinationPort: originPort
        )
        try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: ingressHeader.bytes + scenario.clientBytes)))
    }

    func applyClientTerminal(_ scenario: Phase4OpaqueScenario) throws {
        guard let channel else { throw Phase2FixtureError.closedBeforeExpectedBytes }
        if scenario.clientClosesOutput {
            try phase4BoundedWait(channel.close(mode: .output))
        } else if scenario.clientClosesAbruptly {
            try phase4BoundedWait(channel.close())
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
        observation?.close()
    }
}

private final class Phase4TransparentOpaqueClientHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let terminal: Phase4TerminalObservation
    private let observation: Phase4ExactBytes

    init(terminal: Phase4TerminalObservation, observation: Phase4ExactBytes) {
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

import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Phase4PublicLifecycleTests: XCTestCase {
    func testUntrustedActualPeerIsRejectedBeforeDeclaredMetadata() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let peer = Phase4OpaquePeer(group: group, scenario: .init(
            name: "must-not-connect",
            clientBytes: [],
            serverBytes: [],
            serverReplyBytes: [],
            clientClosesOutput: false,
            serverClosesOutput: false,
            clientClosesAbruptly: false,
            clientStartsStalled: false
        ))
        let sink = Phase4CaptureSink()
        let trustedPeers = try XCTUnwrap(TrustedPeerPolicy(addressesAndCIDRs: ["192.0.2.0/24"]))
        let proxy = try makeProxy(group: group, sink: sink, trustedPeers: trustedPeers)
        let client = Phase4LifecycleClient(group: group)

        do {
            try peer.start()
            let proxyPort = try await proxy.start(port: 0)
            try client.connect(proxyPort: proxyPort)
            try client.sendAllowingUntrustedPeerClosure(
                ingressHeader(destinationPort: peer.localPort).bytes + [0x01]
            )
            try client.waitForClose()
            let snapshot = try sink.wait { $0.connectionFailures.count == 1 }

            XCTAssertEqual(snapshot.connectionFailures.map(\.reason), [.untrustedPeer])
            XCTAssertNil(snapshot.connectionFailures.first?.target)
            await assertNotAccepted(peer.accepted)
            try await finish(proxy: proxy, peer: peer, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
            throw error
        }
    }

    func testClassificationDeadlineFallsBackToExactServerFirstOpaqueFlow() async throws {
        let scenario = Phase4OpaqueScenario(
            name: "deadline-server-first",
            clientBytes: [],
            serverBytes: [0x53, 0x44],
            serverReplyBytes: [],
            clientClosesOutput: false,
            serverClosesOutput: false,
            clientClosesAbruptly: false,
            clientStartsStalled: false
        )
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let peer = Phase4OpaquePeer(group: group, scenario: scenario)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink, classificationDeadline: .milliseconds(20))
        let client = Phase4LifecycleClient(group: group, expectedInboundBytes: scenario.serverBytes)

        do {
            try peer.start()
            let proxyPort = try await proxy.start(port: 0)
            try client.connect(proxyPort: proxyPort)
            try client.send(ingressHeader(destinationPort: peer.localPort).bytes)

            let inboundFuture = try XCTUnwrap(client.inboundBytes)
            let inboundBytes = try await inboundFuture.get()
            let receivedBytes = try await peer.receivedBytes.get()
            XCTAssertEqual(inboundBytes, scenario.serverBytes)
            XCTAssertEqual(receivedBytes, [])
            let snapshot = try sink.wait { $0.opaqueFlows.count == 1 && !$0.opaqueData.isEmpty }
            XCTAssertEqual(snapshot.opaqueData.map(\.direction), [.serverToClient])
            XCTAssertEqual(snapshot.opaqueData.flatMap(\.bytes), scenario.serverBytes)
            try await finish(proxy: proxy, peer: peer, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
            throw error
        }
    }

    func testOversizedTLSClassificationFailsWithoutOriginContact() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let peer = Phase4OpaquePeer(group: group, scenario: .init(
            name: "must-not-connect",
            clientBytes: [],
            serverBytes: [],
            serverReplyBytes: [],
            clientClosesOutput: false,
            serverClosesOutput: false,
            clientClosesAbruptly: false,
            clientStartsStalled: false
        ))
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink, classificationMaximumBytes: 16)
        let client = Phase4LifecycleClient(group: group)

        do {
            try peer.start()
            let proxyPort = try await proxy.start(port: 0)
            try client.connect(proxyPort: proxyPort)
            let oversizedTLS = [UInt8(0x16), 0x03, 0x03, 0x00, 0x64] + Array(repeating: 0, count: 12)
            try client.send(ingressHeader(destinationPort: peer.localPort).bytes + oversizedTLS)
            try client.waitForClose()
            let snapshot = try sink.wait { $0.connectionFailures.count == 1 }

            XCTAssertEqual(snapshot.connectionFailures.map(\.reason), [.classificationFailed])
            await assertNotAccepted(peer.accepted)
            try await finish(proxy: proxy, peer: peer, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
            throw error
        }
    }

    func testStopCancelsPartialHeaderClassificationSetupAndOpaqueStages() async throws {
        for stage in Phase4StopStage.allCases {
            try await assertStop(stage: stage)
        }
    }

    private func assertStop(stage: Phase4StopStage) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let scenario = Phase4OpaqueScenario(
            name: stage.rawValue,
            clientBytes: [],
            serverBytes: [],
            serverReplyBytes: [],
            clientClosesOutput: false,
            serverClosesOutput: false,
            clientClosesAbruptly: false,
            clientStartsStalled: false
        )
        let peer = Phase4OpaquePeer(group: group, scenario: scenario)
        let sink = Phase4CaptureSink()
        let deadline = stage == .opaque ? Duration.milliseconds(20) : .seconds(5)
        let proxy = try makeProxy(group: group, sink: sink, classificationDeadline: deadline)
        let client = Phase4LifecycleClient(group: group)

        do {
            try peer.start()
            let proxyPort = try await proxy.start(port: 0)
            try client.connect(proxyPort: proxyPort)
            try client.send(stage.bytes(destinationPort: peer.localPort))
            try await waitForStageBarrier(stage, peer: peer, sink: sink)
            let clock = ContinuousClock()
            let started = clock.now
            try await proxy.stop()
            let elapsed = started.duration(to: clock.now)
            try client.waitForClose()

            XCTAssertLessThan(elapsed, .seconds(1), stage.rawValue)
            assertCancellation(sink.snapshot, stage: stage)
            peer.stop()
            try await group.shutdownGracefully()
        } catch {
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
            throw error
        }
    }

    private func waitForStageBarrier(
        _ stage: Phase4StopStage,
        peer: Phase4OpaquePeer,
        sink: Phase4CaptureSink
    ) async throws {
        switch stage {
        case .partialHeader:
            try sink.waitForProxyHeaderPending()
        case .classification:
            try sink.waitForClassificationPending()
        case .setup:
            try await peer.accepted.get()
        case .opaque:
            try await peer.accepted.get()
            _ = try sink.wait { $0.opaqueFlows.count == 1 }
        }
    }

    private func assertCancellation(_ snapshot: Phase4CaptureSink.Snapshot, stage: Phase4StopStage) {
        if stage == .opaque {
            XCTAssertEqual(snapshot.opaqueCloses.map(\.reason), [.cancelled])
            XCTAssertEqual(snapshot.opaqueErrors, [])
            return
        }
        XCTAssertEqual(snapshot.connectionFailures.map(\.reason), [.cancelled], stage.rawValue)
        XCTAssertTrue(snapshot.opaqueCloses.isEmpty, stage.rawValue)
    }

    private func assertNotAccepted(_ accepted: EventLoopFuture<Void>) async {
        do {
            try await accepted.get()
            XCTFail("origin must not be contacted")
        } catch {}
    }

    private func makeProxy(
        group: EventLoopGroup,
        sink: Phase4CaptureSink,
        trustedPeers: TrustedPeerPolicy = .loopback,
        classificationMaximumBytes: Int = TrustedProxyV2Ingress.defaultClassificationMaximumBytes,
        classificationDeadline: Duration = .seconds(1)
    ) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: trustedPeers,
            classificationMaximumBytes: classificationMaximumBytes,
            classificationDeadline: classificationDeadline
        ))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(verifyCertificate: false),
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            opaqueCaptureByteLimit: 64
        )
    }

    private func ingressHeader(destinationPort: Int) -> Phase4ProxyV2Header {
        .ipv4(
            source: [192, 0, 2, 40],
            destination: [127, 0, 0, 1],
            sourcePort: 50_003,
            destinationPort: destinationPort
        )
    }

    private func finish(
        proxy: ProxyServer,
        peer: Phase4OpaquePeer,
        client: Phase4LifecycleClient,
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
        client: Phase4LifecycleClient,
        group: EventLoopGroup
    ) async {
        client.stop()
        try? await proxy.stop()
        peer.stop()
        try? await group.shutdownGracefully()
    }
}

private enum Phase4StopStage: String, CaseIterable {
    case partialHeader
    case classification
    case setup
    case opaque

    func bytes(destinationPort: Int) -> [UInt8] {
        let header = Phase4ProxyV2Header.ipv4(
            source: [192, 0, 2, 40],
            destination: [127, 0, 0, 1],
            sourcePort: 50_003,
            destinationPort: destinationPort
        ).bytes
        switch self {
        case .partialHeader:
            return Array(header.prefix(8))
        case .classification:
            return header + [0x47]
        case .opaque:
            return header + [0x01, 0x02]
        case .setup:
            return header + Phase2TLSIngressVectors.http11ClientHello
        }
    }
}

private final class Phase4LifecycleClient {
    private let group: EventLoopGroup
    private let expectedInboundBytes: [UInt8]?
    private var channel: Channel?
    private var closed: Phase2FixtureCompletion<Void>?
    private var observation: Phase4ExactBytes?

    init(group: EventLoopGroup, expectedInboundBytes: [UInt8]? = nil) {
        self.group = group
        self.expectedInboundBytes = expectedInboundBytes
    }

    var inboundBytes: EventLoopFuture<[UInt8]>? { observation?.futureResult }

    func connect(proxyPort: Int) throws {
        let closed = Phase2FixtureCompletion<Void>(eventLoop: group.next())
        let observation = expectedInboundBytes.map {
            Phase4ExactBytes(eventLoop: group.next(), expected: $0, maximumBytes: $0.count)
        }
        self.closed = closed
        self.observation = observation
        channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase4LifecycleClientHandler(
                        closed: closed,
                        observation: observation
                    ))
                }
            }
            .connect(host: "127.0.0.1", port: proxyPort))
    }

    func send(_ bytes: [UInt8]) throws {
        guard let channel else { throw Phase2FixtureError.closedBeforeExpectedBytes }
        try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: bytes)))
    }

    func sendAllowingUntrustedPeerClosure(_ bytes: [UInt8]) throws {
        do {
            try send(bytes)
        } catch ChannelError.ioOnClosedChannel {}
    }

    func waitForClose() throws {
        try closed?.futureResult.wait()
    }

    func stop() {
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        observation?.close()
    }
}

private final class Phase4LifecycleClientHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let closed: Phase2FixtureCompletion<Void>
    private let observation: Phase4ExactBytes?

    init(closed: Phase2FixtureCompletion<Void>, observation: Phase4ExactBytes?) {
        self.closed = closed
        self.observation = observation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        observation?.append(unwrapInboundIn(data).readableBytesView)
    }

    func channelInactive(context: ChannelHandlerContext) {
        observation?.close()
        closed.complete(.success(()))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }
}

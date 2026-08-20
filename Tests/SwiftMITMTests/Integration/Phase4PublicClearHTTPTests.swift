import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Phase4PublicClearHTTPTests: XCTestCase {
    func testTrustedCoalescedClearHTTPRoutesOriginalDestinationAndCapturesHTTP11() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Phase4ClearHTTPOrigin(group: group, scenario: .requestResponse)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink)
        let client = Phase4TransparentClearClient(group: group)

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let response = try client.exchange(
                proxyPort: proxyPort,
                originPort: origin.localPort,
                scenario: .requestResponse,
                delivery: .coalesced
            )
            let originResult = try await origin.result.get()
            let snapshot = try sink.wait {
                $0.requestHeads.count == 1 && $0.responseHeads.count == 1 && $0.eventKinds.contains(.responseEnd)
            }

            XCTAssertEqual(response, Phase4ClearHTTPOrigin.responseBytes)
            XCTAssertEqual(originResult.requestBytes, Phase4ClearHTTPOrigin.requestBytes)
            try assertHTTP11Capture(snapshot, originPort: origin.localPort)
            try await finish(proxy: proxy, origin: origin, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, origin: origin, client: client, group: group)
            throw error
        }
    }

    func testTrustedFragmentedClearWebSocketPreservesWireAndCaptureEquivalence() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Phase4ClearHTTPOrigin(group: group, scenario: .webSocket)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink)
        let client = Phase4TransparentClearClient(group: group)

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let ingressByteCount = 28 + Phase2HTTP1WebSocketPeer.requestBytes.count
            let response = try client.exchange(
                proxyPort: proxyPort,
                originPort: origin.localPort,
                scenario: .webSocket,
                delivery: .fragmented([1, 11, 4, ingressByteCount - 16])
            )
            let originResult = try await origin.result.get()
            let snapshot = try sink.wait {
                $0.webSocketFrames.count == 4 && $0.webSocketCloses.count == 1
            }

            XCTAssertEqual(response, Phase2HTTP1WebSocketPeer.responseBytes + WebSocketWire.serverFrames)
            XCTAssertEqual(originResult.requestBytes, Phase2HTTP1WebSocketPeer.requestBytes)
            XCTAssertEqual(originResult.clientWebSocketFrames, WebSocketWire.clientFrames)
            try assertWebSocketCapture(snapshot, originPort: origin.localPort)
            try await finish(proxy: proxy, origin: origin, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, origin: origin, client: client, group: group)
            throw error
        }
    }

    private func makeProxy(group: EventLoopGroup, sink: Phase4CaptureSink) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            captureBodyLimit: 64
        )
    }

    private func assertHTTP11Capture(_ snapshot: Phase4CaptureSink.Snapshot, originPort: Int) throws {
        let request = try XCTUnwrap(snapshot.requestHeads.first)
        XCTAssertEqual(request.scheme, "http")
        XCTAssertEqual(request.authority, "localhost")
        XCTAssertEqual(request.method, "GET")
        XCTAssertEqual(request.path, "/clear")
        XCTAssertEqual(request.version, .http11)
        XCTAssertEqual(snapshot.responseHeads.map(\.status), [200])
        XCTAssertEqual(snapshot.responseHeads.first?.requestID, request.id)
        assertTarget(request.target, originPort: originPort)
        XCTAssertEqual(snapshot.responseBodies.flatMap(\.bytes), Phase4ClearHTTPOrigin.responseBody)
        let capturedByteCount = snapshot.responseBodies.reduce(0) { $0 + $1.byteCount }
        XCTAssertEqual(capturedByteCount, Phase4ClearHTTPOrigin.responseBody.count)
        XCTAssertEqual(snapshot.eventKinds, [
            .requestHead, .requestEnd, .responseHead, .responseBody, .responseEnd
        ])
    }

    private func assertWebSocketCapture(_ snapshot: Phase4CaptureSink.Snapshot, originPort: Int) throws {
        let request = try XCTUnwrap(snapshot.requestHeads.first)
        XCTAssertEqual(request.scheme, "http")
        XCTAssertEqual(request.path, "/socket")
        XCTAssertEqual(snapshot.responseHeads.map(\.status), [101])
        XCTAssertEqual(snapshot.webSocketOpens.map(\.connectionID), [request.id])
        XCTAssertEqual(snapshot.webSocketFrames.map(\.connectionID), Array(repeating: request.id, count: 4))
        let clientFrames = snapshot.webSocketFrames.filter { $0.direction == .clientToServer }
        let serverFrames = snapshot.webSocketFrames.filter { $0.direction == .serverToClient }
        XCTAssertEqual(clientFrames.map(\.opcode), [.text, .connectionClose])
        XCTAssertEqual(clientFrames.map(\.bytes), [WebSocketWire.clientTextPayload, WebSocketWire.closePayload])
        XCTAssertEqual(serverFrames.map(\.opcode), [.binary, .connectionClose])
        XCTAssertEqual(serverFrames.map(\.bytes), [WebSocketWire.serverBinaryPayload, WebSocketWire.closePayload])
        XCTAssertEqual(snapshot.webSocketCloses.map(\.connectionID), [request.id])
        XCTAssertEqual(snapshot.webSocketCloses.map(\.code), [1000])
        assertTarget(request.target, originPort: originPort)
    }

    private func assertTarget(_ target: CapturedTarget?, originPort: Int) {
        XCTAssertEqual(target?.destination.address, "127.0.0.1")
        XCTAssertEqual(target?.destination.port, originPort)
        XCTAssertEqual(target?.logicalAuthority, "127.0.0.1:\(originPort)")
        XCTAssertNil(target?.tlsServerName)
        XCTAssertEqual(target?.ingressProvenance, .trustedProxyV2)
        XCTAssertEqual(target?.originalClient?.address, "192.0.2.10")
        XCTAssertEqual(target?.originalClient?.port, 50_000)
    }

    private func finish(
        proxy: ProxyServer,
        origin: Phase4ClearHTTPOrigin,
        client: Phase4TransparentClearClient,
        group: EventLoopGroup
    ) async throws {
        client.stop()
        try await proxy.stop()
        origin.stop()
        try await group.shutdownGracefully()
    }

    private func cleanup(
        proxy: ProxyServer,
        origin: Phase4ClearHTTPOrigin,
        client: Phase4TransparentClearClient,
        group: EventLoopGroup
    ) async {
        client.stop()
        try? await proxy.stop()
        origin.stop()
        try? await group.shutdownGracefully()
    }
}

private final class Phase4TransparentClearClient {
    private let group: EventLoopGroup
    private var channel: Channel?
    private var observation: Phase4ExactBytes?

    init(group: EventLoopGroup) {
        self.group = group
    }

    func exchange(
        proxyPort: Int,
        originPort: Int,
        scenario: Phase4ClearHTTPScenario,
        delivery: Phase4ProxyV2Delivery
    ) throws -> [UInt8] {
        let expected = scenario == .requestResponse
            ? Phase4ClearHTTPOrigin.responseBytes
            : Phase2HTTP1WebSocketPeer.responseBytes + WebSocketWire.serverFrames
        let observation = Phase4ExactBytes(eventLoop: group.next(), expected: expected, maximumBytes: expected.count)
        self.observation = observation
        let channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Phase4TransparentClearClientHandler(
                        scenario: scenario,
                        observation: observation
                    ))
                }
            }
            .connect(host: "127.0.0.1", port: proxyPort))
        self.channel = channel
        let request = scenario == .requestResponse
            ? Phase4ClearHTTPOrigin.requestBytes
            : Phase2HTTP1WebSocketPeer.requestBytes
        let header = Phase4ProxyV2Header.ipv4(
            source: [192, 0, 2, 10],
            destination: [127, 0, 0, 1],
            sourcePort: 50_000,
            destinationPort: originPort
        )
        try send(header.bytes + request, delivery: delivery, channel: channel)
        return try observation.futureResult.wait()
    }

    func stop() {
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        observation?.close()
    }

    private func send(_ bytes: [UInt8], delivery: Phase4ProxyV2Delivery, channel: Channel) throws {
        switch delivery {
        case .coalesced:
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: bytes)))
        case .fragmented(let sizes):
            guard sizes.reduce(0, +) == bytes.count else { throw Phase2FixtureError.unexpectedBytes }
            var offset = 0
            for size in sizes {
                try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: bytes[offset ..< offset + size])))
                offset += size
            }
        }
    }
}

private final class Phase4TransparentClearClientHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let scenario: Phase4ClearHTTPScenario
    private let observation: Phase4ExactBytes
    private var sentFrames = false

    init(scenario: Phase4ClearHTTPScenario, observation: Phase4ExactBytes) {
        self.scenario = scenario
        self.observation = observation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        observation.append(unwrapInboundIn(data).readableBytesView)
        if case .webSocket = scenario,
           observation.observedBytes.count >= Phase2HTTP1WebSocketPeer.responseBytes.count,
           !sentFrames {
            sentFrames = true
            context.writeAndFlush(NIOAny(ByteBuffer(bytes: WebSocketWire.clientFrames)), promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        observation.close()
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        observation.close()
        context.close(promise: nil)
    }
}

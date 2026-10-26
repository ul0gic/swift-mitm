import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

private struct Phase6OpaqueResults {
    let clientSent: Int
    let serverReceived: Phase6DigestResult
    let serverSent: Int
    let clientReceived: Phase6DigestResult
}

final class Phase6OpaqueResourceTests: XCTestCase {
    func testLargeOpaqueBidirectionalFlowRetainsEachDirectionIndependently() async throws {
        let permit = await Phase6ResourcePermit.acquire()
        defer { withExtendedLifetime(permit) {} }
        let byteCount = 64 * 1024 * 1024
        let clientSeed: UInt8 = 0x31
        let serverSeed: UInt8 = 0xD2
        let expectedClient = Phase6ResourceStream.digest(seed: clientSeed, count: byteCount)
        let expectedServer = Phase6ResourceStream.digest(seed: serverSeed, count: byteCount)
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 3)
        let sink = Phase6ResourceSink()
        let peer = Phase6OpaqueLoadPeer(group: group, byteCount: byteCount, seed: serverSeed)
        let client = Phase6OpaqueLoadClient(group: group, byteCount: byteCount, seed: clientSeed)
        let proxy = try makeProxy(group: group, sink: sink)

        do {
            try peer.start()
            let proxyPort = try await proxy.start(port: 0)
            let startedAt = Date()
            try client.connect(proxyPort: proxyPort, originPort: peer.localPort)
            _ = try sink.waitForOpaqueOpen()
            let clientSent = try await client.sentBytes.get()
            let serverReceived = try await peer.receivedResult.get()
            let serverSent = try await peer.sentBytes.get()
            let clientReceived = try await client.receivedResult.get()
            let snapshot = try sink.waitForOpaqueTerminal()
            let elapsed = Date().timeIntervalSince(startedAt)
            let results = Phase6OpaqueResults(
                clientSent: clientSent,
                serverReceived: serverReceived,
                serverSent: serverSent,
                clientReceived: clientReceived
            )
            try assertCompletion(
                byteCount: byteCount,
                expectedClient: expectedClient,
                expectedServer: expectedServer,
                results: results,
                originPort: peer.localPort,
                snapshot: snapshot
            )
            writeMeasurement(byteCount: byteCount, elapsed: elapsed, snapshot: snapshot)
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
        } catch {
            await cleanup(proxy: proxy, peer: peer, client: client, group: group)
            throw error
        }
    }

    private func makeProxy(group: EventLoopGroup, sink: Phase6ResourceSink) throws -> ProxyServer {
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
            opaqueCaptureByteLimit: Phase6ResourceStream.captureLimit
        )
    }

    private func assertCompletion(
        byteCount: Int,
        expectedClient: [UInt8],
        expectedServer: [UInt8],
        results: Phase6OpaqueResults,
        originPort: Int,
        snapshot: Phase6ResourceSink.Snapshot
    ) throws {
        XCTAssertEqual(results.clientSent, byteCount)
        XCTAssertEqual(results.serverReceived, .init(byteCount: byteCount, digest: expectedClient))
        XCTAssertEqual(results.serverSent, byteCount)
        XCTAssertEqual(results.clientReceived, .init(byteCount: byteCount, digest: expectedServer))
        XCTAssertEqual(snapshot.clientObservedBytes, byteCount)
        XCTAssertEqual(snapshot.serverObservedBytes, byteCount)
        assertDirectionCapture(snapshot.clientRetainedBytes, seed: 0x31)
        assertDirectionCapture(snapshot.serverRetainedBytes, seed: 0xD2)
        assertDirectionEnds(snapshot.directionEnds, byteCount: byteCount)
        XCTAssertEqual(snapshot.opaqueCloseReasons.map(\.rawValue), [OpaqueFlowCloseReason.completed.rawValue])
        XCTAssertTrue(snapshot.opaqueErrors.isEmpty)
        let flow = try XCTUnwrap(snapshot.opaqueFlow)
        XCTAssertEqual(flow.target.destination.address, "127.0.0.1")
        XCTAssertEqual(flow.target.destination.port, originPort)
        XCTAssertEqual(flow.target.ingressProvenance, .trustedProxyV2)
    }

    private func assertDirectionCapture(_ bytes: [UInt8], seed: UInt8) {
        XCTAssertEqual(bytes.count, Phase6ResourceStream.captureLimit)
        XCTAssertEqual(
            bytes,
            Phase6ResourceStream.bytes(seed: seed, offset: 0, count: Phase6ResourceStream.captureLimit)
        )
    }

    private func assertDirectionEnds(_ ends: [Phase6ResourceSink.DirectionEnd], byteCount: Int) {
        XCTAssertEqual(ends.count, 2)
        for direction in [OpaqueFlowDirection.clientToServer, .serverToClient] {
            let matching = ends.filter { $0.direction == direction }
            XCTAssertEqual(matching.count, 1)
            XCTAssertEqual(matching.first?.byteCount, byteCount)
            XCTAssertEqual(matching.first?.truncated, true)
        }
    }

    private func writeMeasurement(
        byteCount: Int,
        elapsed: TimeInterval,
        snapshot: Phase6ResourceSink.Snapshot
    ) {
        let mebibyte = 1024 * 1024
        let aggregateMiB = Double(byteCount * 2) / Double(mebibyte)
        let message = "PHASE6-OPAQUE-RESOURCE eachDirection=\(byteCount / mebibyte)MiB "
            + "clientObserved=\(snapshot.clientObservedBytes) serverObserved=\(snapshot.serverObservedBytes) "
            + "clientRetained=\(snapshot.clientRetainedBytes.count) "
            + "serverRetained=\(snapshot.serverRetainedBytes.count) "
            + "seconds=\(String(format: "%.3f", elapsed)) "
            + "aggregateMiBps=\(String(format: "%.3f", aggregateMiB / elapsed))\n"
        FileHandle.standardError.write(Data(message.utf8))
    }

    private func cleanup(
        proxy: ProxyServer,
        peer: Phase6OpaqueLoadPeer,
        client: Phase6OpaqueLoadClient,
        group: EventLoopGroup
    ) async {
        client.stop()
        try? await proxy.stop()
        peer.stop()
        try? await group.shutdownGracefully()
    }
}

private final class Phase6OpaqueLoadPeer {
    private let group: EventLoopGroup
    private let byteCount: Int
    private let seed: UInt8
    private let receivedCompletion: Phase2FixtureCompletion<Phase6DigestResult>
    private let sentCompletion: Phase2FixtureCompletion<Int>
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup, byteCount: Int, seed: UInt8) {
        self.group = group
        self.byteCount = byteCount
        self.seed = seed
        receivedCompletion = .init(eventLoop: group.next(), timeout: Phase6ResourceStream.timeout)
        sentCompletion = .init(eventLoop: group.next(), timeout: Phase6ResourceStream.timeout)
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var receivedResult: EventLoopFuture<Phase6DigestResult> { receivedCompletion.futureResult }
    var sentBytes: EventLoopFuture<Int> { sentCompletion.futureResult }

    func start() throws {
        let byteCount = byteCount
        let seed = seed
        let receivedCompletion = receivedCompletion
        let sentCompletion = sentCompletion
        let children = children
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .childChannelOption(.writeBufferWaterMark, value: .init(low: 32 * 1024, high: 64 * 1024))
            .childChannelInitializer { child in
                let identifier = ObjectIdentifier(child)
                children.withLockedValue { $0[identifier] = child }
                child.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return child.eventLoop.makeCompletedFuture {
                    try child.pipeline.syncOperations.addHandler(Phase6BidirectionalLoadHandler(
                        inboundByteCount: byteCount,
                        outboundByteCount: byteCount,
                        outboundSeed: seed,
                        prefix: [],
                        receivedCompletion: receivedCompletion,
                        sentCompletion: sentCompletion
                    ))
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
        try? channel?.close().wait()
        receivedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        sentCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase6OpaqueLoadClient {
    private let group: EventLoopGroup
    private let byteCount: Int
    private let seed: UInt8
    private let receivedCompletion: Phase2FixtureCompletion<Phase6DigestResult>
    private let sentCompletion: Phase2FixtureCompletion<Int>
    private var channel: Channel?

    init(group: EventLoopGroup, byteCount: Int, seed: UInt8) {
        self.group = group
        self.byteCount = byteCount
        self.seed = seed
        receivedCompletion = .init(eventLoop: group.next(), timeout: Phase6ResourceStream.timeout)
        sentCompletion = .init(eventLoop: group.next(), timeout: Phase6ResourceStream.timeout)
    }

    var receivedResult: EventLoopFuture<Phase6DigestResult> { receivedCompletion.futureResult }
    var sentBytes: EventLoopFuture<Int> { sentCompletion.futureResult }

    func connect(proxyPort: Int, originPort: Int) throws {
        let header = Phase4ProxyV2Header.ipv4(
            source: [192, 0, 2, 61],
            destination: [127, 0, 0, 1],
            sourcePort: 50_061,
            destinationPort: originPort
        )
        let byteCount = byteCount
        let seed = seed
        let receivedCompletion = receivedCompletion
        let sentCompletion = sentCompletion
        channel = try ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .channelOption(.writeBufferWaterMark, value: .init(low: 32 * 1024, high: 64 * 1024))
            .channelInitializer { child in
                child.eventLoop.makeCompletedFuture {
                    try child.pipeline.syncOperations.addHandler(Phase6BidirectionalLoadHandler(
                        inboundByteCount: byteCount,
                        outboundByteCount: byteCount,
                        outboundSeed: seed,
                        prefix: header.bytes,
                        receivedCompletion: receivedCompletion,
                        sentCompletion: sentCompletion
                    ))
                }
            }
            .connect(host: "127.0.0.1", port: proxyPort)
            .wait()
    }

    func stop() {
        try? channel?.close().wait()
        receivedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        sentCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

private final class Phase6BidirectionalLoadHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let outboundByteCount: Int
    private let outboundSeed: UInt8
    private let prefix: [UInt8]
    private let receivedCompletion: Phase2FixtureCompletion<Phase6DigestResult>
    private let sentCompletion: Phase2FixtureCompletion<Int>
    private let accumulator: Phase6DigestAccumulator
    private var sent = 0
    private var writeInFlight = false
    private var received = false

    init(
        inboundByteCount: Int,
        outboundByteCount: Int,
        outboundSeed: UInt8,
        prefix: [UInt8],
        receivedCompletion: Phase2FixtureCompletion<Phase6DigestResult>,
        sentCompletion: Phase2FixtureCompletion<Int>
    ) {
        accumulator = Phase6DigestAccumulator(expectedByteCount: inboundByteCount)
        self.outboundByteCount = outboundByteCount
        self.outboundSeed = outboundSeed
        self.prefix = prefix
        self.receivedCompletion = receivedCompletion
        self.sentCompletion = sentCompletion
    }

    func channelActive(context: ChannelHandlerContext) {
        pump(context: context)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            if let result = try accumulator.append(unwrapInboundIn(data)) {
                received = true
                receivedCompletion.complete(.success(result))
            }
        } catch {
            receivedCompletion.complete(.failure(error))
            context.close(promise: nil)
        }
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        if context.channel.isWritable {
            pump(context: context)
        }
        context.fireChannelWritabilityChanged()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let channelEvent = event as? ChannelEvent, case .inputClosed = channelEvent, !received {
            receivedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !received {
            receivedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        if sent < outboundByteCount {
            sentCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        receivedCompletion.complete(.failure(error))
        sentCompletion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func pump(context: ChannelHandlerContext) {
        guard !writeInFlight, sent < outboundByteCount, context.channel.isWritable else { return }
        let count = min(Phase6ResourceStream.chunkSize, outboundByteCount - sent)
        var bytes = Phase6ResourceStream.bytes(seed: outboundSeed, offset: sent, count: count)
        if sent == 0 {
            bytes.insert(contentsOf: prefix, at: 0)
        }
        sent += count
        writeInFlight = true
        let promise = context.eventLoop.makePromise(of: Void.self)
        let handler = NIOLoopBound(self, eventLoop: context.eventLoop)
        let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
        promise.futureResult.whenComplete { result in
            handler.value.writeInFlight = false
            switch result {
            case .success:
                if handler.value.sent == handler.value.outboundByteCount {
                    handler.value.sentCompletion.complete(.success(handler.value.sent))
                    boundContext.value.close(mode: .output, promise: nil)
                } else {
                    handler.value.pump(context: boundContext.value)
                }
            case .failure(let error):
                handler.value.sentCompletion.complete(.failure(error))
                boundContext.value.close(promise: nil)
            }
        }
        context.writeAndFlush(wrapOutboundOut(ByteBuffer(bytes: bytes)), promise: promise)
    }
}

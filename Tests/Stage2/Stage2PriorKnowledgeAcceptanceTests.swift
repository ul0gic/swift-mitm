import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

import SwiftMITM
import SwiftMITMInspection

private final class Stage2ClearH2Origin: Sendable {
    private struct State {
        var listener: Channel?
        var channels: [Channel] = []
        var requests: [HPACKHeaders] = []
    }
    private let state = NIOLockedValueBox(State())
    private let group: EventLoopGroup
    init(group: EventLoopGroup) { self.group = group }
    var port: Int { state.withLockedValue { $0.listener?.localAddress?.port ?? 0 } }
    var requests: [HPACKHeaders] { state.withLockedValue { $0.requests } }

    func start() async throws {
        let listener = try await ServerBootstrap(group: group)
            .childChannelInitializer { [self] channel in
                state.withLockedValue { $0.channels.append(channel) }
                return channel.configureHTTP2Pipeline(
                    mode: .server,
                    connectionConfiguration: .init(),
                    streamConfiguration: .init()
                ) { stream in
                    stream.eventLoop.makeCompletedFuture {
                        try stream.pipeline.syncOperations.addHandler(Stage2ClearH2Handler { headers in
                            self.state.withLockedValue { $0.requests.append(headers) }
                        })
                    }
                }
                .map { _ in () }
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        state.withLockedValue { $0.listener = listener }
    }

    func stop() async throws {
        let channels = state.withLockedValue { $0.channels + [$0.listener].compactMap(\.self) }
        for channel in channels where channel.isActive { try await channel.close().get() }
    }
}

private final class Stage2ClearH2Handler: ChannelInboundHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload
    private let record: @Sendable (HPACKHeaders) -> Void
    init(record: @escaping @Sendable (HPACKHeaders) -> Void) { self.record = record }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case let .headers(frame) = unwrapInboundIn(data),
              frame.headers.contains(name: ":method") else { return }
        record(frame.headers)
        context.write(
            wrapOutboundOut(.headers(.init(headers: HPACKHeaders([
                (":status", "200"),
                ("content-type", "application/grpc")
            ])))),
            promise: nil
        )
        context.write(
            wrapOutboundOut(.data(.init(data: .byteBuffer(ByteBuffer(bytes: [0, 0, 0, 0, 1, 42]))))),
            promise: nil
        )
        context.writeAndFlush(
            wrapOutboundOut(.headers(.init(headers: HPACKHeaders([("grpc-status", "0")]), endStream: true))),
            promise: nil
        )
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { context.close(promise: nil) }
}

final class Stage2PriorKnowledgeAcceptanceTests: XCTestCase {
    func testConfiguredPriorKnowledgeHTTP2ForwardsGRPCAndRejectsChangedAuthority() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Stage2ClearH2Origin(group: group)
        let events = Stage2InspectionEvents()
        let inspector = try HTTPInspector(
            configuration: InspectionConfiguration(budget: InspectionMemoryBudget()),
            sink: events
        )
        let raw = Release022Events()
        var proxy: ProxyServer?
        var client: Channel?
        do {
            try await origin.start()
            let server = try priorKnowledgeProxy(group: group, originPort: origin.port, raw: raw, inspector: inspector)
            proxy = server
            let port = try await server.start(port: 0)
            let muxPromise = group.next().makePromise(of: NIOHTTP2Handler.StreamMultiplexer.self)
            let connection = try await connectPriorKnowledge(group: group, port: port, muxPromise: muxPromise)
            client = connection
            let mux = try phase4BoundedWait(muxPromise.futureResult)
            try await assertPriorKnowledgeExchange(mux: mux, origin: origin, events: events, connection: connection)
            try await connection.close().get()
            try await server.stop()
            try await origin.stop()
            let shutdown = await inspector.shutdown()
            XCTAssertTrue(shutdown.drained)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do {
                if let client, client.isActive {
                    try await client.close().get()
                }; if let proxy {
                    try await proxy.stop()
                }
            } catch { XCTFail("Prior-knowledge proxy teardown failed: \(error)") }
            do { try await origin.stop() } catch { XCTFail("Prior-knowledge origin teardown failed: \(error)") }
            let shutdown = await inspector.shutdown()
            XCTAssertTrue(shutdown.drained)
            do { try await group.shutdownGracefully() } catch {
                XCTFail("Prior-knowledge group teardown failed: \(error)")
            }
            throw failure
        }
    }

    private func priorKnowledgeProxy(
        group: MultiThreadedEventLoopGroup,
        originPort: Int,
        raw: Release022Events,
        inspector: HTTPInspector
    ) throws -> ProxyServer {
        let ingress = try XCTUnwrap(HTTP2PriorKnowledgeIngress(authority: "127.0.0.1:\(originPort)"))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: raw,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .priorKnowledgeHTTP2(ingress),
            captureBodyLimit: 0,
            observationSink: inspector
        )
    }

    private func connectPriorKnowledge(
        group: MultiThreadedEventLoopGroup,
        port: Int,
        muxPromise: EventLoopPromise<NIOHTTP2Handler.StreamMultiplexer>
    ) async throws -> Channel {
        try await ClientBootstrap(group: group)
            .channelInitializer { channel in
                channel.configureHTTP2Pipeline(
                    mode: .client,
                    connectionConfiguration: .init(),
                    streamConfiguration: .init()
                ) { stream in
                    stream.close()
                }
            .map { muxPromise.succeed($0) }
            }
            .connect(host: "127.0.0.1", port: port)
            .get()
    }

    private func assertPriorKnowledgeExchange(
        mux: NIOHTTP2Handler.StreamMultiplexer,
        origin: Stage2ClearH2Origin,
        events: Stage2InspectionEvents,
        connection: Channel
    ) async throws {
        let stream = try stage2H2Stream(mux)
        try stream.write(.headers(.init(
            headers: stage2H2Headers(
                port: origin.port,
                path: "/stage2.Probe/Call",
                scheme: "http",
                host: "127.0.0.1"
            ),
            endStream: true
        )))
        try stream.wait(until: stage2H2Ended)
        XCTAssertEqual(stage2H2Bytes(stream.frames), [0, 0, 0, 0, 1, 42])
        let inspected = try await events.wait { events in
            events.contains {
                if case let .grpc(.status(status)) = $0.payload {
                    status.code == 0
                } else {
                    false
                }
            }
        }
        XCTAssertTrue(inspected.contains {
            if case let .grpc(.message(message)) = $0.payload {
                message.bytes == [42]
            } else {
                false
            }
        })
        XCTAssertEqual(origin.requests.count, 1)
        let bad = Release022H2Stream()
        let badChannel = try phase4BoundedWait(mux.createStreamChannel { channel in
            channel.eventLoop.makeCompletedFuture { try channel.pipeline.syncOperations.addHandler(bad) }
        })
        try bad.write(.headers(.init(
            headers: stage2H2Headers(port: origin.port, path: "/wrong", scheme: "http", host: "localhost"),
            endStream: true
        )))
        try phase4BoundedWait(badChannel.closeFuture)
        XCTAssertEqual(origin.requests.count, 1)
        XCTAssertTrue(connection.isActive)
    }
}

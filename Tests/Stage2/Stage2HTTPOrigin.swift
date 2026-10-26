import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOPosix
import XCTest

import SwiftMITM
import SwiftMITMInspection

struct Stage2OriginRequest: Sendable {
    let head: HTTPRequestHead
    let body: [UInt8]
}

struct Stage2OriginReply: Sendable {
    let status: HTTPResponseStatus
    let headers: HTTPHeaders
    let body: [UInt8]
    let beforeRequestEnd: Bool

    init(
        status: HTTPResponseStatus = .ok,
        headers: HTTPHeaders = [:],
        body: [UInt8] = [],
        beforeRequestEnd: Bool = false
    ) {
        self.status = status
        self.headers = headers
        self.body = body
        self.beforeRequestEnd = beforeRequestEnd
    }
}

final class Stage2HTTPOrigin: Sendable {
    private struct State {
        var accepted = 0
        var requests: [Stage2OriginRequest] = []
        var children: [Channel] = []
        var listener: Channel?
    }

    private let group: EventLoopGroup
    private let state = NIOLockedValueBox(State())
    private let reply: @Sendable (HTTPRequestHead) -> Stage2OriginReply

    init(
        group: EventLoopGroup,
        reply: @escaping @Sendable (HTTPRequestHead) -> Stage2OriginReply = { _ in Stage2OriginReply() }
    ) {
        self.group = group
        self.reply = reply
    }

    var port: Int { state.withLockedValue { $0.listener?.localAddress?.port ?? 0 } }
    var acceptedConnections: Int { state.withLockedValue { $0.accepted } }
    var requests: [Stage2OriginRequest] { state.withLockedValue { $0.requests } }

    func start() async throws {
        let listener = try await ServerBootstrap(group: group)
            .childChannelInitializer { [self] channel in
                state.withLockedValue { state in
                    state.accepted += 1
                    state.children.append(channel)
                }
                return channel.pipeline.configureHTTPServerPipeline().flatMap {
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(
                            Stage2HTTPOriginHandler(reply: self.reply) { request in
                                self.state.withLockedValue { $0.requests.append(request) }
                            }
                        )
                    }
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        state.withLockedValue { $0.listener = listener }
    }

    func stop() async throws {
        let channels = state.withLockedValue { state in
            let channels = state.children + [state.listener].compactMap(\.self)
            state.children.removeAll()
            state.listener = nil
            return channels
        }
        for channel in channels where channel.isActive { try await channel.close().get() }
    }
}

private final class Stage2HTTPOriginHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let reply: @Sendable (HTTPRequestHead) -> Stage2OriginReply
    private let record: @Sendable (Stage2OriginRequest) -> Void
    private var head: HTTPRequestHead?
    private var bytes: [UInt8] = []
    private var replied = false

    init(
        reply: @escaping @Sendable (HTTPRequestHead) -> Stage2OriginReply,
        record: @escaping @Sendable (Stage2OriginRequest) -> Void
    ) {
        self.reply = reply
        self.record = record
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case let .head(head):
            self.head = head
            bytes.removeAll()
            replied = false
            let response = reply(head)
            if response.beforeRequestEnd {
                record(Stage2OriginRequest(head: head, body: []))
                emit(response, context: context)
            }
        case let .body(buffer):
            guard bytes.count <= 4_194_304 - buffer.readableBytes else {
                context.close(promise: nil)
                return
            }
            bytes.append(contentsOf: buffer.readableBytesView)
        case .end:
            guard let head else { context.close(promise: nil); return }
            if !replied {
                record(Stage2OriginRequest(head: head, body: bytes))
                emit(reply(head), context: context)
            }
            self.head = nil
            bytes.removeAll()
        }
    }

    private func emit(_ response: Stage2OriginReply, context: ChannelHandlerContext) {
        replied = true
        var headers = response.headers
        headers.replaceOrAdd(name: "Content-Length", value: String(response.body.count))
        context.write(
            wrapOutboundOut(.head(HTTPResponseHead(version: .http1_1, status: response.status, headers: headers))),
            promise: nil
        )
        if !response.body.isEmpty {
            context.write(wrapOutboundOut(.body(.byteBuffer(ByteBuffer(bytes: response.body)))), promise: nil)
        }
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) { context.close(promise: nil) }
}

struct Stage2HTTPContext {
    let origin: Stage2HTTPOrigin
    let proxy: ProxyServer
    let proxyPort: Int
    let group: EventLoopGroup
    let raw: Release022Events
    let inspected: Stage2InspectionEvents
    let inspector: HTTPInspector
}

func stage2WithHTTPOrigin(
    runtime: HTTPControlRuntime? = nil,
    controlSink: any HTTPControlEventSink? = nil,
    admission: ResourceAdmissionController? = nil,
    reuse: HTTP1ReusePolicy? = nil,
    egressPolicy: EgressPolicy = .init(allowInternal: true),
    inspectorConfiguration: InspectionConfiguration? = nil,
    reply: @escaping @Sendable (HTTPRequestHead) -> Stage2OriginReply = { _ in Stage2OriginReply() },
    body: (Stage2HTTPContext) async throws -> Void
) async throws {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
    let origin = Stage2HTTPOrigin(group: group, reply: reply)
    let raw = Release022Events()
    let inspected = Stage2InspectionEvents()
    let inspector = try HTTPInspector(
        configuration: try inspectorConfiguration ?? InspectionConfiguration(budget: InspectionMemoryBudget()),
        sink: inspected
    )
    let proxy = ProxyServer(
        certificateAuthority: try CertificateAuthority.generate().authority,
        sink: raw,
        group: group,
        egressPolicy: egressPolicy,
        ingress: .explicitHTTP,
        captureBodyLimit: 0,
        observationSink: inspector,
        controlRuntime: runtime,
        controlEventSink: controlSink,
        admission: admission,
        http1ReusePolicy: reuse
    )
    do {
        try await origin.start()
        let port = try await proxy.start(port: 0)
        try await body(Stage2HTTPContext(
            origin: origin,
            proxy: proxy,
            proxyPort: port,
            group: group,
            raw: raw,
            inspected: inspected,
            inspector: inspector
        ))
        try await proxy.stop()
        try await origin.stop()
        let shutdown = await inspector.shutdown()
        XCTAssertTrue(shutdown.drained)
        try await group.shutdownGracefully()
    } catch {
        let failure = error
        do { try await proxy.stop() } catch { XCTFail("Proxy teardown failed: \(error)") }
        do { try await origin.stop() } catch { XCTFail("Origin teardown failed: \(error)") }
        let shutdown = await inspector.shutdown()
        XCTAssertTrue(shutdown.drained)
        do { try await group.shutdownGracefully() } catch { XCTFail("Group teardown failed: \(error)") }
        throw failure
    }
}

func stage2Await(_ predicate: () -> Bool) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: .seconds(5))
    while !predicate() {
        guard clock.now < deadline else { throw Phase2FixtureError.deadlineExceeded }
        try await Task.sleep(for: .milliseconds(10))
    }
}

func stage2AwaitAsync(_ predicate: () async -> Bool) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: .seconds(5))
    while await !predicate() {
        guard clock.now < deadline else { throw Phase2FixtureError.deadlineExceeded }
        try await Task.sleep(for: .milliseconds(10))
    }
}

import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP2
import NIOPosix
import NIOSSL

@testable import SwiftMITM

final class Release022H2Stream: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = HTTP2Frame.FramePayload
    private let condition = NSCondition()
    private var storage: [HTTP2Frame.FramePayload] = []
    private var channel: Channel?
    private var ended = false

    var frames: [HTTP2Frame.FramePayload] { condition.withLock { storage } }

    func handlerAdded(context: ChannelHandlerContext) { channel = context.channel }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        condition.withLock {
            storage.append(unwrapInboundIn(data))
            condition.broadcast()
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        condition.withLock {
            ended = true
            condition.broadcast()
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }

    func wait(until predicate: ([HTTP2Frame.FramePayload]) -> Bool) throws {
        let deadline = Date().addingTimeInterval(3)
        try condition.withLock {
            while !predicate(storage) {
                guard !ended else { throw Phase2FixtureError.closedBeforeExpectedBytes }
                guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
            }
        }
    }

    func write(_ payload: HTTP2Frame.FramePayload) throws {
        guard let channel else { throw Phase2FixtureError.closedBeforeExpectedBytes }
        try phase4BoundedWait(channel.writeAndFlush(payload))
    }
}

final class Release022HTTP2Origin: @unchecked Sendable {
    let authority: CertificateAuthority
    private let group: EventLoopGroup
    private let condition = NSCondition()
    private var streams: [Release022H2Stream] = []
    private let children = NIOLockedValueBox<[Channel]>([])
    private var listener: Channel?

    init(group: EventLoopGroup) throws {
        self.group = group
        authority = try CertificateAuthority.generate().authority
    }

    var port: Int { listener?.localAddress?.port ?? 0 }

    func start() throws {
        let leaf = try authority.leaf(forHost: "localhost")
        var tls = TLSConfiguration.makeServerConfiguration(certificateChain: leaf.certificateChain, privateKey: leaf.privateKey)
        tls.applicationProtocols = ["h2"]
        let ssl = try NIOSSLContext(configuration: tls)
        let children = children
        listener = try phase4BoundedWait(ServerBootstrap(group: group)
            .childChannelInitializer { channel in
                children.withLockedValue { $0.append(channel) }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(NIOSSLServerHandler(context: ssl))
                }.flatMap {
                    channel.configureHTTP2Pipeline(mode: .server, connectionConfiguration: .init(), streamConfiguration: .init()) { stream in
                        let observation = Release022H2Stream()
                        return stream.eventLoop.makeCompletedFuture {
                            try stream.pipeline.syncOperations.addHandler(observation)
                            self.condition.withLock {
                                self.streams.append(observation)
                                self.condition.broadcast()
                            }
                        }
                    }.map { _ in () }
                }
            }
            .bind(host: "127.0.0.1", port: 0))
    }

    func stream(_ index: Int) throws -> Release022H2Stream {
        let deadline = Date().addingTimeInterval(3)
        return try condition.withLock {
            while streams.count <= index {
                guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
            }
            return streams[index]
        }
    }

    func stop() {
        for child in children.withLockedValue({ $0 }) { try? phase4BoundedWait(child.close()) }
        if let listener { try? phase4BoundedWait(listener.close()) }
    }
}

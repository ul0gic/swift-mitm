import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

import SwiftMITM

final class Release022Events: CaptureEventSink, @unchecked Sendable {
    private let condition = NSCondition()
    private var storage: [CaptureEvent] = []

    var events: [CaptureEvent] { condition.withLock { storage } }

    func reset() { condition.withLock { storage.removeAll(keepingCapacity: true) } }

    func receive(_ event: CaptureEvent) {
        condition.withLock {
            storage.append(event)
            condition.broadcast()
        }
    }

    @discardableResult
    func wait(until predicate: ([CaptureEvent]) -> Bool) throws -> [CaptureEvent] {
        let deadline = Date().addingTimeInterval(3)
        return try condition.withLock {
            while !predicate(storage) {
                guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
            }
            return storage
        }
    }
}

final class Release022WireObservation: @unchecked Sendable {
    private let condition = NSCondition()
    private var storage: [UInt8] = []
    private var closed = false

    var bytes: [UInt8] { condition.withLock { storage } }

    func reset() { condition.withLock { storage.removeAll(keepingCapacity: true) } }

    func append(_ bytes: ByteBufferView) {
        condition.withLock {
            storage.append(contentsOf: bytes)
            condition.broadcast()
        }
    }

    func end() {
        condition.withLock {
            closed = true
            condition.broadcast()
        }
    }

    func waitForBytes(_ count: Int) throws -> [UInt8] {
        try wait { $0.count >= count }
    }

    func wait(until predicate: ([UInt8]) -> Bool) throws -> [UInt8] {
        let deadline = Date().addingTimeInterval(3)
        return try condition.withLock {
            while !predicate(storage) {
                guard !closed else { throw Phase2FixtureError.closedBeforeExpectedBytes }
                guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
            }
            return storage
        }
    }

    func waitForClose() throws {
        let deadline = Date().addingTimeInterval(3)
        try condition.withLock {
            while !closed {
                guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
            }
        }
    }
}

final class Release022WireHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    let observation: Release022WireObservation

    init(_ observation: Release022WireObservation) { self.observation = observation }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        observation.append(unwrapInboundIn(data).readableBytesView)
    }

    func channelInactive(context: ChannelHandlerContext) {
        observation.end()
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case ChannelEvent.inputClosed = event {
            observation.end()
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        observation.end()
        context.close(promise: nil)
    }
}

final class Release022WireOrigin {
    let received = Release022WireObservation()
    private let group: EventLoopGroup
    private let accepted: Phase2FixtureCompletion<Channel>
    private let children = NIOLockedValueBox<[(channel: Channel, received: Release022WireObservation)]>([])
    private var listener: Channel?

    init(group: EventLoopGroup) {
        self.group = group
        accepted = Phase2FixtureCompletion(eventLoop: group.next(), timeout: .seconds(3))
    }

    var port: Int { listener?.localAddress?.port ?? 0 }
    var acceptedConnections: Int { children.withLockedValue { $0.count } }

    func start(host: String = "127.0.0.1") throws {
        let received = received
        let accepted = accepted
        let children = children
        listener = try phase4BoundedWait(ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try children.withLockedValue { peers in
                        let observation = peers.isEmpty ? received : Release022WireObservation()
                        try channel.pipeline.syncOperations.addHandler(Release022WireHandler(observation))
                        peers.append((channel, observation))
                    }
                    accepted.complete(.success(channel))
                }
            }
            .bind(host: host, port: 0))
    }

    func write(_ bytes: [UInt8]) throws {
        let peer = try phase4BoundedWait(accepted.futureResult)
        try phase4BoundedWait(peer.writeAndFlush(ByteBuffer(bytes: bytes)))
    }

    func closeOutput() throws {
        let peer = try phase4BoundedWait(accepted.futureResult)
        try phase4BoundedWait(peer.close(mode: .output))
    }

    func waitForConnection(at index: Int) throws -> (channel: Channel, received: Release022WireObservation) {
        let deadline = Date().addingTimeInterval(3)
        repeat {
            let peer: (channel: Channel, received: Release022WireObservation)? = children.withLockedValue {
                $0.indices.contains(index) ? $0[index] : nil
            }
            if let peer {
                return peer
            }
            guard Date() < deadline else { throw Phase2FixtureError.deadlineExceeded }
            Thread.sleep(forTimeInterval: 0.01)
        } while true
    }

    func stop() {
        accepted.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        for peer in children.withLockedValue({ $0 }) { try? phase4BoundedWait(peer.channel.close()) }
        if let listener {
            try? phase4BoundedWait(listener.close())
        }
    }
}

func release022WireClient(group: EventLoopGroup, port: Int, observation: Release022WireObservation) throws -> Channel {
    try phase4BoundedWait(ClientBootstrap(group: group)
        .connectTimeout(.seconds(2))
        .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
        .channelInitializer { channel in
            channel.eventLoop.makeCompletedFuture {
                try channel.pipeline.syncOperations.addHandler(Release022WireHandler(observation))
            }
        }
        .connect(host: "127.0.0.1", port: port))
}

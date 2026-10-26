import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

final class Phase4SilentRawOrigin {
    private let group: EventLoopGroup
    private let acceptedCompletion: Phase2FixtureCompletion<Void>
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup) {
        self.group = group
        acceptedCompletion = Phase2FixtureCompletion(eventLoop: group.next(), timeout: .seconds(5))
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var accepted: EventLoopFuture<Void> { acceptedCompletion.futureResult }

    func start() throws {
        let acceptedCompletion = acceptedCompletion
        let children = children
        channel = try phase4BoundedWait(ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                children.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                acceptedCompletion.complete(.success(()))
                return channel.eventLoop.makeSucceededVoidFuture()
            }
            .bind(host: "127.0.0.1", port: 0))
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? phase4BoundedWait($0.close()) }
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        acceptedCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

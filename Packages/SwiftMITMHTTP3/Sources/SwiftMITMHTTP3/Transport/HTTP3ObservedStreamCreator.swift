import NIOCore
import NIOQUIC
import NIOQUICHelpers

struct HTTP3ObservedStreamCreator: NIOQUICHelpers.QUICStreamCreator {
    let native: NIOQUIC.QUICStreamCreator
    let resources: HTTP3ResourceTracker

    func assumeIsolated() -> Isolated {
        Isolated(native: native.assumeIsolated(), resources: resources)
    }

    func createBidirectionalStream<Output: Sendable>(
        streamInitializer: @escaping @Sendable (QUICStreamInitializerParameters) -> EventLoopFuture<Output>
    ) -> EventLoopFuture<Output> {
        native.createBidirectionalStream { parameters in
            do { try Self.observe(parameters.channel, resources: resources) } catch {
                return parameters.channel.eventLoop.makeFailedFuture(error)
            }
            return streamInitializer(parameters)
        }
    }

    func createUnidirectionalStream<Output: Sendable>(
        streamInitializer: @escaping @Sendable (QUICStreamInitializerParameters) -> EventLoopFuture<Output>
    ) -> EventLoopFuture<Output> {
        native.createUnidirectionalStream { parameters in
            do { try Self.observe(parameters.channel, resources: resources) } catch {
                return parameters.channel.eventLoop.makeFailedFuture(error)
            }
            return streamInitializer(parameters)
        }
    }

    static func observe(_ channel: any Channel, resources: HTTP3ResourceTracker) throws {
        try resources.track(channel)
        try channel.pipeline.syncOperations.addHandler(HTTP3RawReadMeter(resources: resources), position: .first)
    }

    struct Isolated: IsolatedQUICStreamCreator {
        let native: NIOQUIC.QUICStreamCreator.Isolated
        let resources: HTTP3ResourceTracker

        func createBidirectionalStream<Output: Sendable>(
            streamInitializer: @escaping (QUICStreamInitializerParameters) -> EventLoopFuture<Output>
        ) -> EventLoopFuture<Output> {
            native.createBidirectionalStream { parameters in
                do { try HTTP3ObservedStreamCreator.observe(parameters.channel, resources: resources) } catch {
                    return parameters.channel.eventLoop.makeFailedFuture(error)
                }
                return streamInitializer(parameters)
            }
        }

        func createUnidirectionalStream<Output: Sendable>(
            streamInitializer: @escaping (QUICStreamInitializerParameters) -> EventLoopFuture<Output>
        ) -> EventLoopFuture<Output> {
            native.createUnidirectionalStream { parameters in
                do { try HTTP3ObservedStreamCreator.observe(parameters.channel, resources: resources) } catch {
                    return parameters.channel.eventLoop.makeFailedFuture(error)
                }
                return streamInitializer(parameters)
            }
        }
    }
}

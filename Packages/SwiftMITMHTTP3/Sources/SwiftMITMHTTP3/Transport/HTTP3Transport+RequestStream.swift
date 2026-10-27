import NIOCore

extension HTTP3Transport {
    func openRequestStream(_ setup: HTTP3OriginSetup, session: HTTP3OriginSession, connection: any Channel) {
        guard !closing, !session.draining, !setup.completed,
              let exchange = exchanges[setup.requestID], !exchange.setupCancelled else {
            setup.fail(CancellationError())
            return
        }
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let owned = NIOLoopBound((setup, session), eventLoop: eventLoop)
        do {
            let handler = try connection.pipeline.syncOperations.handler(type: HTTP3NativeHandler.self)
            handler.createRequestStream { parameters in
                let channel = parameters.channel
                return channel.setOption(ChannelOptions.autoRead, value: false).flatMap {
                    state.value.initializeOriginStream(channel, setup: owned.value.0)
                }
            }
            .whenComplete { result in
                let (setup, session) = owned.value
                switch result {
                case let .success(channel):
                    guard !state.value.closing, !setup.completed,
                          let exchange = state.value.exchanges[setup.requestID], !exchange.setupCancelled else {
                        channel.close(promise: nil)
                        setup.fail(CancellationError())
                        return
                    }
                    setup.succeed(HTTP3OriginStream(sessionID: session.id, channel: channel, target: session.target))
                case let .failure(error):
                    if error is CancellationError {
                        setup.fail(error)
                    } else {
                        setup.fail(error as? HTTP3ProxyError ?? .upstreamConnectionFailed)
                    }
                }
            }
        } catch { setup.fail(error as? HTTP3ProxyError ?? .upstreamConnectionFailed) }
    }

    private func initializeOriginStream(
        _ channel: any Channel,
        setup: HTTP3OriginSetup
    ) -> EventLoopFuture<any Channel> {
        guard !closing, !setup.completed, let initialize = setup.initialize else {
            channel.close(promise: nil)
            return eventLoop.makeFailedFuture(CancellationError())
        }
        return initialize(channel).map { channel }.flatMapError { error in
            channel.close(promise: nil)
            return channel.eventLoop.makeFailedFuture(error)
        }
    }
}

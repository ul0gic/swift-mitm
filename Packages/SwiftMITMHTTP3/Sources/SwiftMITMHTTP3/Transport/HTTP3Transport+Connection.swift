import Foundation
import NIOCore
import NIOHTTP3
import NIOQUIC

extension HTTP3Transport {
    func connect(_ session: HTTP3OriginSession) {
        let handler: HTTP3DatagramHandler?
        switch session.address {
        case .v4: handler = clientHandler
        case .v6: handler = clientHandlerIPv6
        case .unixDomainSocket: handler = nil
        }
        guard !closing, let handler else {
            connectionFailed(session, error: .upstreamConnectionFailed)
            return
        }
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let owned = NIOLoopBound(session, eventLoop: eventLoop)
        session.ownership.timedOut = {
            state.value.connectionFailed(owned.value, error: .handshakeTimedOut)
            owned.value.channel?.close(promise: nil)
        }
        if session.exclusiveRequestID == nil {
            session.ownership.startDeadline(on: eventLoop, duration: configuration.limits.handshakeDeadline)
        }
        handler.createOutboundConnection(
            serverName: session.identity.host, remoteAddress: session.address,
            connectionInitializer: { channel, creator in
                channel.eventLoop.makeCompletedFuture {
                    let session = owned.value
                    session.channel = channel
                    try state.value.trackConnection(channel, ownership: session.ownership)
                    guard !state.value.closing, !session.draining else { throw CancellationError() }
                    try state.value.installOrigin(channel, creator: creator, session: session)
                }
            },
            inboundStreamInitializer: { Self.receiveStream($0, state: state) }
        ).whenComplete { result in
            state.value.connected(result, session: owned.value)
        }
    }

    private func connected(
        _ result: Result<(any Channel, NIOQUIC.QUICStreamCreator), any Error>,
        session: HTTP3OriginSession
    ) {
        switch result {
        case let .success((channel, _)):
            guard !closing, !session.draining, channel.remoteAddress == session.address else {
                connectionFailed(session, error: .destinationUnavailable)
                channel.close(promise: nil)
                return
            }
            if !session.connectionCompleted {
                session.connectionCompleted = true
                session.promise.succeed(channel)
            }
        case let .failure(error):
            let failure = session.ownership.failure ?? error as? HTTP3ProxyError ?? .upstreamConnectionFailed
            connectionFailed(session, error: failure)
        }
    }

    private func installOrigin(
        _ channel: any Channel,
        creator: NIOQUIC.QUICStreamCreator,
        session: HTTP3OriginSession
    ) throws {
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let owned = NIOLoopBound(session, eventLoop: eventLoop)
        channel.closeFuture.whenComplete { _ in
            state.value.connectionFailed(owned.value, error: owned.value.ownership.failure ?? .upstreamConnectionFailed)
        }
        var native = HTTP3ClientConfiguration.defaults
        native.inboundMemory = inboundMemoryConfiguration()
        let handler = HTTP3NativeHandler.client(
            eventLoop: eventLoop, configuration: native,
            settings: .init(qpackMaximumTableCapacity: 0, qpackBlockedStreams: 0, maximumFieldSectionSize: 32_768, h3Datagram: false),
            streamCreator: HTTP3ObservedStreamCreator(native: creator, resources: resources), logger: logger
        ) { $0.channel.close() }
        let observer = HTTP3ConnectionObserver(
            ownership: session.ownership,
            receivedGoaway: {
                owned.value.draining = true
                state.value.removeFixedEntry(owned.value)
                state.value.resources.observeGoaway()
            },
            failed: { state.value.connectionFailed(owned.value, error: $0) }
        )
        try channel.pipeline.syncOperations.addHandlers([handler, observer])
    }

    func connectionFailed(_ session: HTTP3OriginSession, error: HTTP3ProxyError) {
        let wasPresent = sessions.removeValue(forKey: session.id) != nil
        session.draining = true
        removeFixedEntry(session)
        session.ownership.finishHandshake()
        if !session.ownership.attached { session.ownership.release() }
        if !session.connectionCompleted {
            session.connectionCompleted = true
            session.promise.fail(error)
        }
        if wasPresent, !closing, !session.closedIntentionally {
            emitFailure(id: session.id, target: session.target, error: error)
        }
        cancelExchanges(matching: { $0.sessionID == session.id }, reason: .transportFailure(error))
    }

    func removeFixedEntry(_ session: HTTP3OriginSession) {
        if fixedSessions[session.downstreamID] == session.id { fixedSessions.removeValue(forKey: session.downstreamID) }
    }
}

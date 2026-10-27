import Foundation
import NIOCore
import NIOHTTP3
import NIOQUIC
import SwiftMITM

extension HTTP3Transport {
    static func configureDownstream(
        _ channel: any Channel,
        creator: NIOQUIC.QUICStreamCreator,
        state: NIOLoopBound<HTTP3Transport>
    ) -> EventLoopFuture<Void> {
        channel.eventLoop.makeCompletedFuture {
            let transport = state.value
            guard !transport.closing else {
                try transport.trackConnection(channel, ownership: nil)
                throw CancellationError()
            }
            let ownership: HTTP3ConnectionOwnership
            do {
                ownership = try HTTP3ConnectionOwnership(
                    resources: transport.resources, configuration: transport.configuration, destination: nil
                )
            } catch {
                try transport.trackConnection(channel, ownership: nil)
                transport.emitFailure(id: UUID(), target: nil, error: .resourceLimit, downstream: true)
                throw error
            }
            do { try transport.trackConnection(channel, ownership: ownership) } catch {
                ownership.release()
                throw error
            }
            let downstream = transport.makeDownstream(channel)
            transport.downstreams[downstream.id] = downstream
            let owned = NIOLoopBound(ownership, eventLoop: channel.eventLoop)
            ownership.timedOut = {
                state.value.emitFailure(id: downstream.id, target: downstream.target, error: .handshakeTimedOut, downstream: true)
                channel.close(promise: nil)
            }
            ownership.startDeadline(on: channel.eventLoop, duration: transport.configuration.limits.handshakeDeadline)
            channel.closeFuture.whenComplete { _ in
                owned.value.finishHandshake()
                state.value.closeDownstream(downstream.id, error: owned.value.failure ?? .protocolViolation)
            }
            try transport.installDownstream(channel, creator: creator, downstream: downstream, ownership: ownership)
        }
    }

    private func makeDownstream(_ channel: any Channel) -> HTTP3DownstreamConnection {
        let route = configuration.route
        let peer = channel.remoteAddress.flatMap { address -> CapturedNetworkEndpoint? in
            guard let host = address.ipAddress, let port = address.port else { return nil }
            return CapturedNetworkEndpoint(address: host, port: port)
        }
        return HTTP3DownstreamConnection(
            id: UUID(), channel: channel,
            target: CapturedTarget(
                destination: route.destination, logicalAuthority: route.authority,
                tlsServerName: Self.wireServerName(route.identity), ingressProvenance: .fixedRouteHTTP3, originalClient: peer
            )
        )
    }

    private func installDownstream(
        _ channel: any Channel,
        creator: NIOQUIC.QUICStreamCreator,
        downstream: HTTP3DownstreamConnection,
        ownership: HTTP3ConnectionOwnership
    ) throws {
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let connection = NIOLoopBound(downstream, eventLoop: eventLoop)
        var native = HTTP3ServerConfiguration.defaults
        native.inboundMemory = inboundMemoryConfiguration()
        let handler = HTTP3NativeHandler.server(
            eventLoop: eventLoop, configuration: native,
            settings: .init(qpackMaximumTableCapacity: 0, qpackBlockedStreams: 0, maximumFieldSectionSize: 32_768, h3Datagram: false),
            streamCreator: HTTP3ObservedStreamCreator(native: creator, resources: resources), logger: logger
        ) { parameters in
            let stream = parameters.channel
            return stream.setOption(ChannelOptions.autoRead, value: false).flatMap {
                HTTP3Exchange.install(on: stream, downstream: connection.value, transport: state.value)
            }
        }
        let observer = HTTP3ConnectionObserver(
            ownership: ownership,
            receivedGoaway: { state.value.resources.observeGoaway() },
            failed: { error in
                state.value.emitFailure(id: downstream.id, target: downstream.target, error: error, downstream: true)
            }
        )
        try channel.pipeline.syncOperations.addHandlers([handler, observer])
    }

    func closeDownstream(_ id: UUID, error: HTTP3ProxyError) {
        downstreams.removeValue(forKey: id)
        fixedSessions.removeValue(forKey: id)
        cancelExchanges(matching: { $0.downstream.id == id }, reason: .transportFailure(error))
        for session in sessions.values where session.downstreamID == id {
            session.draining = true
            session.closedIntentionally = true
            session.channel?.close(promise: nil)
        }
    }
}

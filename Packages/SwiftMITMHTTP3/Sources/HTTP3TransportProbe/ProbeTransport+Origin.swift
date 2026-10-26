import NIOCore
import NIOHTTP3
import NIOHTTPTypes
import NIOQUIC

extension ProbeTransport {
    func connect(
        part: HTTPRequestPart,
        downstream: Channel,
        requestRelay: NIOLoopBound<ProbeRelay<HTTPRequestPart>>
    ) -> EventLoopFuture<Channel> {
        guard case let .head(head) = part, head.scheme == "https", head.authority == arguments.authority,
              head.method != .connect, head.extendedConnectProtocol == nil else {
            return downstream.eventLoop.makeFailedFuture(ProbeError.invalidRequest)
        }
        do { try arguments.authorize() }
        catch { return downstream.eventLoop.makeFailedFuture(error) }
        guard origins.count < 128, let clientHandler else {
            return downstream.eventLoop.makeFailedFuture(ProbeError.capacity)
        }
        nextConnectionID += 1
        let id = nextConnectionID
        origins[id] = Connection()
        let state = NIOLoopBound(self, eventLoop: downstream.eventLoop)
        return clientHandler.value
.createOutboundConnection(
    serverName: arguments.hostname,
    remoteAddress: arguments.originAddress,
    connectionInitializer: { channel, creator in
                Self.configureOriginConnection(channel, creator: creator, state: state, id: id, downstream: downstream)
            },
    inboundStreamInitializer: Self.receiveStream
)
.flatMap { connection, _ in
            guard downstream.isActive else {
                return connection.close().flatMapThrowing { throw ChannelError.ioOnClosedChannel }
            }
            do {
                let handler = try connection.pipeline.syncOperations.handler(type: ProbeHTTP3Handler.self)
                return handler.createRequestStream { parameters in
                    Self.configureOriginStream(
                        parameters.channel,
                        downstream: downstream,
                        state: state,
                        requestRelay: requestRelay
                    )
                }
            } catch { return downstream.eventLoop.makeFailedFuture(error) }
        }
.flatMapError { error in
            if let connection = state.value.origins.removeValue(forKey: id)?.channel {
                connection.close(promise: nil)
            }
            return downstream.eventLoop.makeFailedFuture(error)
        }
    }

    private static func configureOriginConnection(
        _ channel: Channel,
        creator: NIOQUIC.QUICStreamCreator,
        state: NIOLoopBound<ProbeTransport>,
        id: UInt64,
        downstream: Channel
    ) -> EventLoopFuture<Void> {
        channel.eventLoop.makeCompletedFuture {
            state.value.origins[id] = Connection(channel: channel)
            downstream.closeFuture.whenComplete { _ in channel.close(promise: nil) }
            channel.closeFuture.whenComplete { _ in
                state.value.origins.removeValue(forKey: id)
                downstream.close(promise: nil)
            }
            let handler = ProbeHTTP3Handler.client(
                eventLoop: channel.eventLoop,
                configuration: .defaults,
                settings: .init(
                    qpackMaximumTableCapacity: 0,
                    qpackBlockedStreams: 0,
                    maximumFieldSectionSize: 32_768,
                    h3Datagram: false
                ),
                streamCreator: creator,
                logger: state.value.logger,
                inboundPushStreamInitializer: { $0.channel.close() }
            )
            try channel.pipeline.syncOperations.addHandlers([ProbeHandshakeDeadline(), handler])
        }
    }

    private static func configureOriginStream(
        _ stream: Channel,
        downstream: Channel,
        state: NIOLoopBound<ProbeTransport>,
        requestRelay: NIOLoopBound<ProbeRelay<HTTPRequestPart>>
    ) -> EventLoopFuture<Channel> {
        stream.setOption(ChannelOptions.autoRead, value: false).flatMap {
            stream.eventLoop.makeCompletedFuture {
                let response = ProbeRelay<HTTPResponsePart>(admission: state.value.admission, destination: downstream)
                let boundResponse = NIOLoopBound(response, eventLoop: stream.eventLoop)
                response.resumePeer = { requestRelay.value.readIfReady() }
                requestRelay.value.resumePeer = { boundResponse.value.readIfReady() }
                try stream.pipeline.syncOperations.addHandler(response)
                return stream
            }
        }
    }
}

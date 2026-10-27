import Logging
import NIOCore
import NIOHTTP3
import NIOHTTPTypes
import NIOPosix
import NIOQUIC
import SwiftMITM

typealias ProbeHTTP3Handler = HTTP3ConnectionHandler<NIOQUIC.QUICStreamCreator>
typealias ProbeQUICHandler = QUICHandler<QUICStreamChannels>

final class ProbeTransport {
    let arguments: ProbeArguments
    let admission: ResourceAdmissionController
    let metrics: ProbeMetrics
    let logger = Logger(label: "SwiftMITM.HTTP3TransportProbe")
    var clientHandler: NIOLoopBound<ProbeQUICHandler>?
    var serverHandler: NIOLoopBound<ProbeQUICHandler>?
    var origins: [ObjectIdentifier: EventLoopFuture<Channel>] = [:]

    init(arguments: ProbeArguments) throws {
        self.arguments = arguments
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        self.admission = admission
        metrics = ProbeMetrics(admission: admission)
    }

    static func configuration(server: ProbeArguments? = nil, metrics: ProbeMetrics) -> QUICConfiguration {
        var configuration: QUICConfiguration
        if let server {
            // NIO requires the X509 mode tag; its injected Authenticator supplies the in-memory leaf identity.
            configuration = .forServer(
                serverName: server.hostname,
                authenticationConfiguration: .x509Certificates(
                    certificateChainFilePath: server.certificatePath,
                    privateKeyFilePath: server.keyPath
                ),
                applicationProtocols: ["h3"]
            )
        } else {
            configuration = .forClient(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: nil),
                applicationProtocols: ["h3"]
            )
        }
        configuration.initialMaxStreamsBidi = 128
        configuration.initialMaxStreamDataBidiLocal = 65_536
        configuration.initialMaxStreamDataBidiRemote = 65_536
        configuration.initialMaxStreamDataUni = 65_536
        configuration.initialMaxData = 1024 * 1024
        configuration.maxDatagramFrameSize = 0
        configuration.maximumBytesPerRead = 16_384
        configuration.maximumPendingWriteBytes = 65_536
        configuration.streamStorageReservationFactory = { bytes in
            try metrics.reserve(bytes, kind: .nativeStream)
        }
        return configuration
    }

    func inboundMemoryConfiguration() -> HTTP3InboundMemoryConfiguration {
        let metrics = metrics
        return HTTP3InboundMemoryConfiguration(
            maximumEncodedFieldSectionBytes: 32_768,
            maximumBytesPerRead: 16_384,
            makeStorageReservation: { bytes in try metrics.reserve(bytes, kind: .parser) }
        )
    }

    static func receiveStream(_ channel: Channel, metrics: ProbeMetrics) -> EventLoopFuture<Void> {
        channel.eventLoop.flatSubmit {
            guard let parent = channel.parent else {
                return channel.eventLoop.makeFailedFuture(ProbeError.missingHandler)
            }
            do {
                try channel.pipeline.syncOperations.addHandler(ProbeRawReadMeter(metrics: metrics), position: .first)
                let handler = try parent.pipeline.syncOperations.handler(type: ProbeHTTP3Handler.self)
                return handler.inboundStreamReceived(channel)
            } catch { return channel.eventLoop.makeFailedFuture(error) }
        }
    }

    static func startClient(
        state: NIOLoopBound<ProbeTransport>,
        loop: EventLoop,
        verifier: AsyncVerifier
    ) -> EventLoopFuture<Channel> {
        DatagramBootstrap(group: loop)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    let noMoreConnections: @Sendable () -> Void = { state.value.clientHandler = nil }
                    let handler = ProbeQUICHandler(
                        channel: channel,
                        quicConfiguration: configuration(metrics: state.value.metrics),
                        asyncVerifier: verifier,
                        authenticator: nil,
                        logger: state.value.logger,
                        inboundConnectionInitializer: { channel, _ in channel.close() },
                        inboundStreamInitializer: { receiveStream($0, metrics: state.value.metrics) },
                        noMoreConnections: noMoreConnections
                    )
                    state.value.clientHandler = NIOLoopBound(handler, eventLoop: loop)
                    try channel.pipeline.syncOperations.addHandler(handler)
                }
            }
            .bind(host: "127.0.0.1", port: 0)
    }

    static func startServer(
        state: NIOLoopBound<ProbeTransport>,
        loop: EventLoop,
        authenticator: Authenticator
    ) -> EventLoopFuture<Channel> {
        DatagramBootstrap(group: loop)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    let noMoreConnections: @Sendable () -> Void = { state.value.serverHandler = nil }
                    let handler = ProbeQUICHandler(
                        channel: channel,
                        quicConfiguration: configuration(server: state.value.arguments, metrics: state.value.metrics),
                        connectionLimits: .perHandler(
                            activeLimit: 128,
                            handshakeLimit: 64,
                            newConnectionRateLimit: nil
                        ),
                        asyncVerifier: nil,
                        authenticator: authenticator,
                        logger: state.value.logger,
                        inboundConnectionInitializer: { connection, creator in
                            configureServerConnection(connection, creator: creator, state: state)
                        },
                        inboundStreamInitializer: { receiveStream($0, metrics: state.value.metrics) },
                        noMoreConnections: noMoreConnections
                    )
                    state.value.serverHandler = NIOLoopBound(handler, eventLoop: loop)
                    try channel.pipeline.syncOperations.addHandler(handler)
                }
            }
            .bind(host: "127.0.0.1", port: state.value.arguments.port)
    }

    private static func configureServerConnection(
        _ channel: Channel,
        creator: NIOQUIC.QUICStreamCreator,
        state: NIOLoopBound<ProbeTransport>
    ) -> EventLoopFuture<Void> {
        channel.eventLoop.makeCompletedFuture {
            var configuration = HTTP3ServerConfiguration.defaults
            configuration.inboundMemory = state.value.inboundMemoryConfiguration()
            let handler = ProbeHTTP3Handler.server(
                eventLoop: channel.eventLoop,
                configuration: configuration,
                settings: .init(
                    qpackMaximumTableCapacity: 0,
                    qpackBlockedStreams: 0,
                    maximumFieldSectionSize: 32_768,
                    h3Datagram: false
                ),
                streamCreator: creator,
                logger: state.value.logger
            ) { parameters in
                let stream = parameters.channel
                return stream.setOption(ChannelOptions.autoRead, value: false).flatMap {
                    stream.eventLoop.makeCompletedFuture {
                        let relay = ProbeRelay<HTTPRequestPart>(metrics: state.value.metrics) { part, source, relay in
                            state.value.connect(part: part, downstream: source, requestRelay: relay)
                        }
                        try stream.pipeline.syncOperations.addHandler(relay)
                    }
                }
            }
            try channel.pipeline.syncOperations.addHandlers([ProbeHandshakeDeadline(), handler])
        }
    }
}

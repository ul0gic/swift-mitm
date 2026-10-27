import NIOCore
import NIOHTTP3
import NIOPosix
import NIOQUIC
import SwiftMITM

extension HTTP3Transport {
    func nativeConfiguration(server: Bool) -> QUICConfiguration {
        var native: QUICConfiguration
        if server {
            let identity = configuration.route.identity
            // The direct handler initializer uses the injected in-memory Authenticator, never these mode-tag paths.
            native = .forServer(
                serverName: identity.host,
                authenticationConfiguration: .x509Certificates(
                    certificateChainFilePath: "/dev/null", privateKeyFilePath: "/dev/null"
                ),
                applicationProtocols: ["h3"]
            )
            native.receivedServerNameValidator = { received in
                guard let received else { return true }
                guard (try? SocketAddress(ipAddress: received, port: identity.port)) == nil,
                      !received.contains(":"), !received.hasPrefix("[") else { return false }
                return HTTPAuthority(authority: "\(received):\(identity.port)")?.host == identity.host
            }
        } else {
            native = .forClient(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: nil), applicationProtocols: ["h3"]
            )
        }
        native.initialMaxStreamsBidi = configuration.limits.maximumRequestStreamsPerConnection
        native.initialMaxStreamDataBidiLocal = 65_536
        native.initialMaxStreamDataBidiRemote = 65_536
        native.initialMaxStreamDataUni = 65_536
        native.initialMaxData = 1024 * 1024
        native.maxDatagramFrameSize = 0
        native.maximumBytesPerRead = 16_384
        native.maximumPendingWriteBytes = 65_536
        let resources = resources
        native.streamStorageReservationFactory = { try resources.reserve($0, kind: .nativeStream) }
        return native
    }

    static func startClient(
        state: NIOLoopBound<HTTP3Transport>,
        loop: any EventLoop,
        verifier: AsyncVerifier,
        ipv6: Bool
    ) -> EventLoopFuture<any Channel> {
        DatagramBootstrap(group: loop)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    let handler = HTTP3DatagramHandler(
                        channel: channel,
                        quicConfiguration: state.value.nativeConfiguration(server: false),
                        asyncVerifier: verifier,
                        authenticator: nil,
                        logger: state.value.logger,
                        inboundConnectionInitializer: { channel, _ in channel.close() },
                        inboundStreamInitializer: { receiveStream($0, state: state) },
                        noMoreConnections: {
                            if ipv6 { state.value.clientHandlerIPv6 = nil } else { state.value.clientHandler = nil }
                        }
                    )
                    if ipv6 { state.value.clientHandlerIPv6 = handler } else { state.value.clientHandler = handler }
                    try channel.pipeline.syncOperations.addHandler(handler)
                }
            }
            .bind(host: ipv6 ? "::" : "0.0.0.0", port: 0)
            .map { channel in
                if ipv6 { state.value.clientSocketIPv6 = channel } else { state.value.clientSocket = channel }
                return channel
            }
    }

    static func startServer(
        state: NIOLoopBound<HTTP3Transport>,
        loop: any EventLoop,
        authenticator: Authenticator,
        address: SocketAddress
    ) -> EventLoopFuture<any Channel> {
        DatagramBootstrap(group: loop)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    let limits = state.value.configuration.limits
                    let handler = HTTP3DatagramHandler(
                        channel: channel,
                        quicConfiguration: state.value.nativeConfiguration(server: true),
                        connectionLimits: .perHandler(
                            activeLimit: limits.maximumConnections,
                            handshakeLimit: limits.maximumPendingHandshakes,
                            newConnectionRateLimit: nil
                        ),
                        asyncVerifier: nil,
                        authenticator: authenticator,
                        logger: state.value.logger,
                        inboundConnectionInitializer: { channel, creator in
                            configureDownstream(channel, creator: creator, state: state)
                        },
                        inboundStreamInitializer: { receiveStream($0, state: state) },
                        noMoreConnections: { state.value.completeListener() }
                    )
                    state.value.serverHandler = handler
                    try channel.pipeline.syncOperations.addHandler(handler)
                }
            }
            .bind(to: address)
            .map { channel in
                state.value.serverSocket = channel
                return channel
            }
    }
}

import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL
import NIOTLS

@testable import SwiftMITM

final class Phase3SilentSettingsTLSOrigin: @unchecked Sendable {
    let caCertificatePEM: String
    let hostname = "localhost"

    private let group: EventLoopGroup
    private let sslContext: NIOSSLContext
    private let receivedBytesCompletion: Phase2FixtureCompletion<Void>
    private let children = NIOLockedValueBox<[ObjectIdentifier: Channel]>([:])
    private var channel: Channel?

    init(group: EventLoopGroup) throws {
        self.group = group
        let ca = try CertificateAuthority(commonName: "SwiftMITM Phase 3 Silent Settings Root")
        caCertificatePEM = ca.caCertificatePEM
        let leaf = try ca.leaf(forHost: hostname)
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: leaf.certificateChain,
            privateKey: leaf.privateKey
        )
        configuration.applicationProtocols = ["h2"]
        sslContext = try NIOSSLContext(configuration: configuration)
        receivedBytesCompletion = Phase2FixtureCompletion(eventLoop: group.next())
    }

    var localPort: Int { channel?.localAddress?.port ?? 0 }
    var receivedHTTP2Bytes: EventLoopFuture<Void> { receivedBytesCompletion.futureResult }

    func start() throws {
        let sslContext = sslContext
        let receivedBytesCompletion = receivedBytesCompletion
        let children = children
        channel = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let identifier = ObjectIdentifier(channel)
                children.withLockedValue { $0[identifier] = channel }
                channel.closeFuture.whenComplete { _ in
                    children.withLockedValue { _ = $0.removeValue(forKey: identifier) }
                }
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers([
                        NIOSSLServerHandler(context: sslContext),
                        ApplicationProtocolNegotiationHandler { result, channel in
                            guard case .negotiated("h2") = result else {
                                return channel.eventLoop.makeFailedFuture(Phase2FixtureError.unexpectedBytes)
                            }
                            return channel.eventLoop.makeCompletedFuture {
                                try channel.pipeline.syncOperations.addHandler(
                                    Phase3HTTP2BytesSignalHandler(completion: receivedBytesCompletion)
                                )
                            }
                        }
                    ])
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
    }

    func stop() {
        children.withLockedValue { Array($0.values) }.forEach { try? $0.close().wait() }
        try? channel?.close().wait()
        receivedBytesCompletion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }
}

extension Phase3ProxyHTTP2WebSocketClient {
    func waitForSettingsFailure(
        proxyPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String
    ) throws -> Bool {
        let channel = try openTunnel(
            proxyPort: proxyPort,
            originHost: originHost,
            originPort: originPort
        )
        defer { try? channel.close().wait() }
        let setup = try configureTLSAndHTTP2(
            channel: channel,
            originHost: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM,
            capabilityTimeout: .seconds(7)
        )
        do {
            _ = try setup.capability.wait()
            return false
        } catch {
            return true
        }
    }
}

private final class Phase3HTTP2BytesSignalHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let completion: Phase2FixtureCompletion<Void>

    init(completion: Phase2FixtureCompletion<Void>) {
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        if buffer.readableBytes > 0 {
            completion.complete(.success(()))
        }
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

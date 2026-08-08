import Foundation
import NIOCore
import NIOHTTP2
import NIOPosix

public final class HTTP2ProxyBridge: Sendable {
    public struct UpstreamTarget: Sendable {
        public let host: String
        public let port: Int

        public init(host: String, port: Int) {
            self.host = host
            self.port = port
        }
    }

    private let group: EventLoopGroup
    private let upstream: UpstreamTarget
    private let sink: CaptureEventSink
    private let targetWindowSize: Int
    private let captureBodyLimit: Int

    public init(
        group: EventLoopGroup,
        upstream: UpstreamTarget,
        sink: CaptureEventSink,
        targetWindowSize: Int = 65535,
        captureBodyLimit: Int = 0
    ) {
        self.group = group
        self.upstream = upstream
        self.sink = sink
        self.targetWindowSize = targetWindowSize
        self.captureBodyLimit = captureBodyLimit
    }

    public func start(host: String = "127.0.0.1", port: Int = 0) -> EventLoopFuture<Channel> {
        ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { [self] clientChannel in
                configureClientConnection(clientChannel)
            }
            .bind(host: host, port: port)
    }

    private var streamConfiguration: NIOHTTP2Handler.StreamConfiguration {
        var config = NIOHTTP2Handler.StreamConfiguration()
        config.targetWindowSize = targetWindowSize
        return config
    }

    private var connectionConfiguration: NIOHTTP2Handler.ConnectionConfiguration {
        var config = NIOHTTP2Handler.ConnectionConfiguration()
        config.targetWindowSize = targetWindowSize
        return config
    }

    private func configureClientConnection(_ clientChannel: Channel) -> EventLoopFuture<Void> {
        let loop = clientChannel.eventLoop
        return connectUpstream(on: loop).flatMap { [self] upstreamMux in
            clientChannel.configureHTTP2Pipeline(
                mode: .server,
                connectionConfiguration: connectionConfiguration,
                streamConfiguration: streamConfiguration
            ) { [self] inboundStream in
                glue(inbound: inboundStream, upstreamMux: upstreamMux)
            }
            .map { _ in () }
        }
    }

    private func connectUpstream(on loop: EventLoop) -> EventLoopFuture<NIOHTTP2Handler.StreamMultiplexer> {
        ClientBootstrap(group: loop)
            .connect(host: upstream.host, port: upstream.port)
            .flatMap { [self] channel in
                channel.configureHTTP2Pipeline(
                    mode: .client,
                    connectionConfiguration: connectionConfiguration,
                    streamConfiguration: streamConfiguration
                ) { pushStream in
                    pushStream.close()
                }
            }
    }

    private func glue(
        inbound: Channel,
        upstreamMux: NIOHTTP2Handler.StreamMultiplexer
    ) -> EventLoopFuture<Void> {
        H2StreamGlue.glue(
            inboundStream: inbound,
            upstreamMux: upstreamMux,
            authority: "\(upstream.host):\(upstream.port)",
            sink: sink,
            captureBodyLimit: captureBodyLimit
        )
    }
}

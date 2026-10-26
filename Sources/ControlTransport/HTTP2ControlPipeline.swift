import NIOCore
import NIOHTTP2

enum HTTP2ControlPipeline {
    static func configure(
        channel: Channel,
        mode: NIOHTTP2Handler.ParserMode,
        configuration: NIOHTTP2Handler.ConnectionConfiguration,
        initializer: @escaping NIOChannelInitializer
    ) -> EventLoopFuture<NIOLoopBound<HTTP2StreamMultiplexer>> {
        channel.eventLoop.makeCompletedFuture {
            var configuration = configuration
            configuration.initialSettings.removeAll { $0.parameter == .initialWindowSize }
            configuration.initialSettings.append(HTTP2Setting(parameter: .initialWindowSize, value: 65_535))
            let gate = NIOLoopBound(HTTP2ReadCreditGate(), eventLoop: channel.eventLoop)
            let multiplexer = HTTP2StreamMultiplexer(
                mode: mode,
                channel: channel,
                targetWindowSize: 65_535,
                inboundStreamInitializer: { stream in
                    stream.eventLoop
                        .makeCompletedFuture {
                            try stream.pipeline.syncOperations.addHandler(HTTP2StreamReadCreditHandler(gate: gate))
                        }
                        .flatMap { initializer(stream) }
                }
            )
            try channel.pipeline.syncOperations.addHandlers([
                NIOHTTP2Handler(mode: mode, connectionConfiguration: configuration),
                gate.value,
                multiplexer
            ])
            return NIOLoopBound(multiplexer, eventLoop: channel.eventLoop)
        }
    }
}

import Foundation
import NIOCore
import NIOHTTP2

enum H2StreamGlue {
    struct Configuration: Sendable {
        let authority: String
        var target: CapturedTarget?
        let sink: CaptureEventSink
        var captureBodyLimit = 0
        var extendedConnectEnabled = false
        var controlServer: ProxyServer?
    }

    static func glue(
        inboundStream: Channel,
        upstreamMux: NIOHTTP2Handler.StreamMultiplexer,
        configuration: Configuration
    ) -> EventLoopFuture<Void> {
        do {
            let reservation = try configuration.controlServer?.admission?.acquire(.stream)
            inboundStream.closeFuture.whenComplete { _ in reservation?.release() }
        } catch {
            return inboundStream.close().flatMapThrowing { throw error }
        }
        if let controlServer = configuration.controlServer, let runtime = controlServer.controlRuntime,
           !runtime.configurationSnapshot.rules.isEmpty {
            return inboundStream.eventLoop.makeCompletedFuture {
                try inboundStream.pipeline.syncOperations.addHandler(HTTP2ControlStreamSelector(
                    server: controlServer,
                    upstreamMux: upstreamMux,
                    authority: configuration.authority,
                    target: configuration.target,
                    extendedConnectEnabled: configuration.extendedConnectEnabled
                ))
            }
        }
        return glueRaw(inboundStream: inboundStream, upstreamMux: upstreamMux, configuration: configuration)
    }

    private static func glueRaw(
        inboundStream: Channel,
        upstreamMux: NIOHTTP2Handler.StreamMultiplexer,
        configuration: Configuration
    ) -> EventLoopFuture<Void> {
        let loop = inboundStream.eventLoop
        let requestID = UUID()
        let errorState = HTTP2StreamErrorState()
        let streamContext = NIOLoopBound(
            HTTP2WebSocketStreamContext(
                requestID: requestID,
                sink: configuration.sink,
                captureLimit: configuration.captureBodyLimit,
                extendedConnectEnabled: configuration.extendedConnectEnabled
            ),
            eventLoop: loop
        )
        let pair = NIOLoopBound(GlueHandler.matchedPair(propagateInputClosed: false), eventLoop: loop)
        let capture: @Sendable (HTTP2CaptureTapHandler.Direction) -> HTTP2CaptureTapHandler = { direction in
            HTTP2CaptureTapHandler(
                direction: direction,
                requestID: requestID,
                authority: configuration.authority,
                target: configuration.target,
                sink: configuration.sink,
                captureBodyLimit: configuration.captureBodyLimit,
                errorState: errorState,
                streamContext: streamContext
            )
        }

        return upstreamMux.createStreamChannel { upstreamStream in
            upstreamStream.eventLoop.makeCompletedFuture {
                streamContext.value.configurePairedStreams(request: inboundStream, response: upstreamStream)
                try upstreamStream.pipeline.syncOperations.addHandlers([
                    capture(.response),
                    pair.value.1
                ])
            }
        }
        .flatMap { _ in
            loop.makeCompletedFuture {
                try inboundStream.pipeline.syncOperations.addHandlers([
                    capture(.request),
                    pair.value.0
                ])
            }
        }
    }
}

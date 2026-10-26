import Foundation
import NIOCore
import NIOHTTP2

enum H2StreamGlue {
    static func glue(
        inboundStream: Channel,
        upstreamMux: NIOHTTP2Handler.StreamMultiplexer,
        authority: String,
        target: CapturedTarget? = nil,
        sink: CaptureEventSink,
        captureBodyLimit: Int = 0,
        extendedConnectEnabled: Bool = false
    ) -> EventLoopFuture<Void> {
        let loop = inboundStream.eventLoop
        let requestID = UUID()
        let errorState = HTTP2StreamErrorState()
        let streamContext = NIOLoopBound(
            HTTP2WebSocketStreamContext(
                requestID: requestID,
                sink: sink,
                captureLimit: captureBodyLimit,
                extendedConnectEnabled: extendedConnectEnabled
            ),
            eventLoop: loop
        )
        let pair = NIOLoopBound(GlueHandler.matchedPair(propagateInputClosed: false), eventLoop: loop)

        return upstreamMux.createStreamChannel { upstreamStream in
            upstreamStream.eventLoop.makeCompletedFuture {
                streamContext.value.configurePairedStreams(request: inboundStream, response: upstreamStream)
                try upstreamStream.pipeline.syncOperations.addHandlers([
                    HTTP2CaptureTapHandler(
                        direction: .response,
                        requestID: requestID,
                        authority: authority,
                        target: target,
                        sink: sink,
                        captureBodyLimit: captureBodyLimit,
                        errorState: errorState,
                        streamContext: streamContext
                    ),
                    pair.value.1
                ])
            }
        }
        .flatMap { _ in
            loop.makeCompletedFuture {
                try inboundStream.pipeline.syncOperations.addHandlers([
                    HTTP2CaptureTapHandler(
                        direction: .request,
                        requestID: requestID,
                        authority: authority,
                        target: target,
                        sink: sink,
                        captureBodyLimit: captureBodyLimit,
                        errorState: errorState,
                        streamContext: streamContext
                    ),
                    pair.value.0
                ])
            }
        }
    }
}

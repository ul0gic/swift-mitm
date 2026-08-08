import Foundation
import NIOCore
import NIOHTTP2

enum H2StreamGlue {
    static func glue(
        inboundStream: Channel,
        upstreamMux: NIOHTTP2Handler.StreamMultiplexer,
        authority: String,
        sink: CaptureEventSink,
        captureBodyLimit: Int = 0
    ) -> EventLoopFuture<Void> {
        let loop = inboundStream.eventLoop
        let requestID = UUID()
        let pair = NIOLoopBound(GlueHandler.matchedPair(), eventLoop: loop)

        return upstreamMux.createStreamChannel { upstreamStream in
            upstreamStream.eventLoop.makeCompletedFuture {
                try upstreamStream.pipeline.syncOperations.addHandlers([
                    HTTP2CaptureTapHandler(
                        direction: .response,
                        requestID: requestID,
                        authority: authority,
                        sink: sink,
                        captureBodyLimit: captureBodyLimit
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
                        sink: sink,
                        captureBodyLimit: captureBodyLimit
                    ),
                    pair.value.0
                ])
            }
        }
    }
}

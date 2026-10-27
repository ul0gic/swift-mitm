import Foundation
import NIOCore
import NIOHTTP2

extension ProxyServer {
    func connectControlledHTTP2(
        target: HTTPForwardTarget,
        requestID: UUID,
        errorState: HTTP2StreamErrorState,
        downstream: Channel,
        on loop: EventLoop
    ) -> EventLoopFuture<HTTPForwardConnection> {
        guard target.scheme == "https" || target.scheme == "http",
              let destination = ConnectionTarget(explicitConnectAuthority: target.authority),
              let token = connectionRegistry.beginSetup() else {
            return loop.makeFailedFuture(HTTPControlError.unsupportedEdit)
        }
        downstream.closeFuture.whenComplete { _ in token.cancel() }
        let connection: EventLoopFuture<ResolvedUpstreamConnection>
        if target.scheme == "https" {
            let runtime: ProxyTLSRuntime
            do { runtime = try activeTLSRuntime() } catch { token.complete(); return loop.makeFailedFuture(error) }
            connection = connectUpstreamTLS(
                target: destination,
                applicationProtocols: [.http2],
                tlsRuntime: runtime,
                setupToken: token,
                on: loop
            )
        } else {
            connection = connectUpstreamRaw(target: destination, on: loop, setupToken: token).map {
                ResolvedUpstreamConnection(
                    connection: NegotiatedUpstreamConnection(
                        channel: $0.channel,
                        applicationProtocol: .http2,
                        alpnHandler: nil
                    ),
                    target: $0.target
                )
            }
        }
        return connection
            .flatMap { resolved in
                self.configureControlledHTTP2(
                    resolved,
                    target: target,
                    requestID: requestID,
                    errorState: errorState,
                    downstream: downstream
                )
            }
            .always { _ in token.complete() }
    }

    private func configureControlledHTTP2(
        _ resolved: ResolvedUpstreamConnection,
        target: HTTPForwardTarget,
        requestID: UUID,
        errorState: HTTP2StreamErrorState,
        downstream: Channel
    ) -> EventLoopFuture<HTTPForwardConnection> {
        let connection = resolved.connection
        guard downstream.isActive else {
            return connection.channel.close().flatMapThrowing { throw HTTPControlError.cancelled }
        }
        guard connection.applicationProtocol == .http2 else {
            return connection.channel.close().flatMapThrowing { throw HTTPControlError.unsupportedEdit }
        }
        var configuration = NIOHTTP2Handler.ConnectionConfiguration()
        configuration.targetWindowSize = targetWindowSize
        return HTTP2ControlPipeline.configure(
            channel: connection.channel,
            mode: .client,
            configuration: configuration
        ) { $0.close() }
            .flatMap { mux in
                connection.releaseBufferedReads().flatMap {
                    mux.value.createStreamChannel { stream in
                        self.configureControlledHTTP2Stream(
                            stream,
                            resolved: resolved,
                            target: target,
                            requestID: requestID,
                            errorState: errorState
                        )
                    }
                }
            }
            .map {
                HTTPForwardConnection(channel: $0, target: resolved.target.capturedTarget, usesHTTP2Codec: true)
            }
            .flatMapError { error in connection.channel.close().flatMapThrowing { throw error } }
    }

    private func configureControlledHTTP2Stream(
        _ stream: Channel,
        resolved: ResolvedUpstreamConnection,
        target: HTTPForwardTarget,
        requestID: UUID,
        errorState: HTTP2StreamErrorState
    ) -> EventLoopFuture<Void> {
        let connection = resolved.connection
        stream.closeFuture.whenComplete { _ in connection.channel.close(promise: nil) }
        return stream.setOption(ChannelOptions.autoRead, value: false).flatMap {
            stream.eventLoop.makeCompletedFuture {
                let gate = try connection.channel.pipeline.syncOperations.handler(type: HTTP2ReadCreditGate.self)
                try stream.pipeline.syncOperations.addHandler(HTTP2StreamReadCreditHandler(
                    gate: NIOLoopBound(gate, eventLoop: stream.eventLoop)
                ))
                try stream.pipeline.syncOperations.addHandler(HTTP2CaptureTapHandler(
                    direction: .response,
                    requestID: requestID,
                    authority: target.authority,
                    target: resolved.target.capturedTarget,
                    sink: self.sink,
                    captureBodyLimit: self.captureBodyLimit,
                    errorState: errorState
                ))
            }
        }
    }
}

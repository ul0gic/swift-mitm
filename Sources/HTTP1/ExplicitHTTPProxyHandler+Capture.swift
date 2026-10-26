import Foundation
import NIOHTTP1

extension ExplicitHTTPProxyHandler {
    func captureRequestHead(_ exchange: Exchange, target: CapturedTarget) {
        guard let forwardTarget = exchange.target else { return }
        exchange.published = true
        correlator.publish(id: exchange.id)
        sink.receive(.requestHead(CapturedRequestHead(
            id: exchange.id,
            timestamp: exchange.timestamp,
            scheme: forwardTarget.scheme,
            authority: forwardTarget.authority,
            method: exchange.head.method.rawValue,
            path: forwardTarget.requestTarget,
            version: .http11,
            headers: Self.fields(exchange.head.headers),
            target: target
        )))
    }

    func captureRequest(_ part: HTTPServerRequestPart, exchange: Exchange) {
        guard exchange.published else { return }
        switch part {
        case .head:
            break
        case .body(let body):
            sink.receive(.requestBodyChunk(
                requestID: exchange.id,
                bytes: exchange.requestBody.take(body.readableBytesView),
                byteCount: body.readableBytes
            ))
        case .end(let trailers):
            if let trailers, !trailers.isEmpty {
                sink.receive(.requestTrailers(requestID: exchange.id, headers: Self.fields(trailers)))
            }
            sink.receive(.requestEnd(requestID: exchange.id, truncated: exchange.requestBody.truncated))
        }
    }

    func diagnose(reason: CapturedConnectionFailureReason, stage: ConnectionFailureStage, code: ConnectionFailureCode) {
        guard !connectionFailureEmitted else { return }
        connectionFailureEmitted = true
        sink.receive(.connectionFailure(CapturedConnectionFailure(
            id: connectionID,
            timestamp: Date(),
            reason: reason,
            stage: stage,
            code: code
        )))
    }

    func diagnoseConnect(_ error: Error) {
        switch error {
        case ProxyConnectionError.egressBlocked:
            diagnose(reason: .destinationUnavailable, stage: .routing, code: .egressDenied)
        case ProxyConnectionError.setupTimedOut:
            diagnose(reason: .timedOut, stage: .connect, code: .setupTimedOut)
        case ProxyConnectionError.serverStopping:
            diagnose(reason: .cancelled, stage: .connect, code: .serverStopping)
        default:
            diagnose(reason: .upstreamConnectionFailed, stage: .connect, code: .upstreamConnectFailed)
        }
    }
}

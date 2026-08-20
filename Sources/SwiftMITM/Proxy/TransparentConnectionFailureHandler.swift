import Foundation
import NIOCore
import NIOSSL

final class TransparentConnectionFailureHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = NIOAny

    private let sink: CaptureEventSink
    private var target: CapturedTarget?
    private var terminal = false

    init(sink: CaptureEventSink) {
        self.sink = sink
    }

    func updateTarget(_ target: CapturedTarget) {
        self.target = target
    }

    func cancel() {
        emit(.cancelled)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        emit(.cancelled)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        emit(Self.reason(for: error))
        context.close(promise: nil)
    }

    private func emit(_ reason: CapturedConnectionFailureReason) {
        guard !terminal else { return }
        terminal = true
        sink.receive(.connectionFailure(CapturedConnectionFailure(
            id: UUID(),
            timestamp: Date(),
            reason: reason,
            target: target
        )))
    }

    private static func reason(for error: Error) -> CapturedConnectionFailureReason {
        if let ingressError = error as? TrustedProxyV2IngressError {
            return reason(for: ingressError)
        }
        if let parserError = error as? ProxyV2ParserError {
            return reason(for: parserError)
        }
        if error is TransparentApplicationClassificationError {
            return .classificationFailed
        }
        if let connectionError = error as? ProxyConnectionError {
            return reason(for: connectionError)
        }
        if error is NIOSSLExtraError || error is NIOSSLError {
            return .tlsHandshakeFailed
        }
        return .upstreamConnectionFailed
    }

    private static func reason(for error: TrustedProxyV2IngressError) -> CapturedConnectionFailureReason {
        switch error {
        case .untrustedPeer, .peerAddressUnavailable:
            return .untrustedPeer
        case .targetUnavailable:
            return .destinationUnavailable
        case .deadlineExceeded:
            return .timedOut
        }
    }

    private static func reason(for error: ProxyV2ParserError) -> CapturedConnectionFailureReason {
        switch error {
        case .unsupportedVersion, .unsupportedCommand, .unsupportedAddressFamily, .unsupportedTransport:
            return .unsupportedProxyTransport
        default:
            return .malformedProxyMetadata
        }
    }

    private static func reason(for error: ProxyConnectionError) -> CapturedConnectionFailureReason {
        switch error {
        case .egressBlocked:
            return .destinationUnavailable
        case .serverStopping:
            return .cancelled
        case .setupTimedOut:
            return .timedOut
        }
    }
}

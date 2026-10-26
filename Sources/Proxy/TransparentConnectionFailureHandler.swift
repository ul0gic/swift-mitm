import Foundation
import NIOCore
import NIOPosix
import NIOSSL
import NIOTLS

final class TransparentConnectionFailureHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = NIOAny

    private let sink: CaptureEventSink
    let id: UUID
    private var target: CapturedTarget?
    private var stage = ConnectionFailureStage.admission
    private var terminal = false
    private var terminalError: Error?
    private var handoffPromise: EventLoopPromise<Void>?
    private var context: ChannelHandlerContext?
    private var deadlineTask: Scheduled<Void>?

    init(sink: CaptureEventSink, connectionID: UUID = UUID()) {
        self.sink = sink
        id = connectionID
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        deadlineTask?.cancel()
        deadlineTask = nil
        self.context = nil
    }

    func handoffFuture(on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        if let handoffPromise { return handoffPromise.futureResult }
        let promise = eventLoop.makePromise(of: Void.self)
        handoffPromise = promise
        if let terminalError {
            promise.fail(terminalError)
        } else if terminal {
            promise.succeed(())
        }
        return promise.futureResult
    }

    func updateStage(_ stage: ConnectionFailureStage) {
        self.stage = stage
    }

    func beginTLSSetup(deadline: TimeAmount, setupToken: ProxyConnectionSetupToken) {
        guard !terminal, let context else { return }
        stage = .downstreamTLS
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        let channel = context.channel
        deadlineTask = context.eventLoop.scheduleTask(in: deadline) {
            owner.value.emit(.timedOut, code: .setupTimedOut, error: ProxyConnectionError.setupTimedOut)
            setupToken.cancel()
            channel.close(promise: nil)
        }
    }

    func complete() {
        guard !terminal else { return }
        terminal = true
        deadlineTask?.cancel()
        deadlineTask = nil
        handoffPromise?.succeed(())
    }

    func updateTarget(_ target: CapturedTarget) {
        self.target = target
    }

    func cancel() {
        emit(.cancelled, code: .serverStopping, error: ProxyConnectionError.serverStopping)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        emit(.cancelled, code: .transportFailure, error: ProxyALPNError.channelClosedBeforeNegotiation)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        emit(Self.reason(for: error), code: Self.code(for: error), error: error)
        context.close(promise: nil)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted = event {
            stage = .negotiation
        }
        context.fireUserInboundEventTriggered(event)
    }

    private func emit(
        _ reason: CapturedConnectionFailureReason,
        code: ConnectionFailureCode,
        error: Error
    ) {
        guard !terminal else { return }
        terminal = true
        terminalError = error
        deadlineTask?.cancel()
        deadlineTask = nil
        handoffPromise?.fail(error)
        sink.receive(.connectionFailure(CapturedConnectionFailure(
            id: id,
            timestamp: Date(),
            reason: reason,
            target: target,
            stage: stage,
            code: code
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
        if error is ProxyALPNError {
            return .tlsHandshakeFailed
        }
        if let settingsError = error as? HTTP2InitialSettingsProbeError {
            return settingsError == .deadlineExceeded ? .timedOut : .transportFailure
        }
        if let connectionError = connectionError(for: error) {
            return reason(for: connectionError)
        }
        if error is NIOSSLExtraError || error is NIOSSLError {
            return .tlsHandshakeFailed
        }
        return .upstreamConnectionFailed
    }

    private static func code(for error: Error) -> ConnectionFailureCode {
        if let connectionError = connectionError(for: error) {
            switch connectionError {
            case .egressBlocked: return .egressDenied
            case .serverStopping: return .serverStopping
            case .setupTimedOut: return .setupTimedOut
            }
        }
        if let ingressError = error as? TrustedProxyV2IngressError {
            switch ingressError {
            case .untrustedPeer, .peerAddressUnavailable: return .untrustedPeer
            case .targetUnavailable: return .destinationUnavailable
            case .deadlineExceeded: return .setupTimedOut
            }
        }
        if error is ProxyV2ParserError { return .invalidProxyMetadata }
        if error is TransparentApplicationClassificationError { return .classificationFailed }
        if let settingsError = error as? HTTP2InitialSettingsProbeError {
            return settingsError == .deadlineExceeded ? .setupTimedOut : .transportFailure
        }
        if let alpnError = error as? ProxyALPNError {
            switch alpnError {
            case .malformedClientHello: return .malformedClientHello
            case .noSupportedClientProtocol, .unsupportedProtocol: return .unsupportedProtocol
            case .protocolMismatch: return .protocolMismatch
            case .channelClosedBeforeNegotiation: return .transportFailure
            }
        }
        if error is NIOSSLExtraError || error is NIOSSLError { return .tlsHandshakeFailed }
        return .upstreamConnectFailed
    }

    private static func connectionError(for error: Error) -> ProxyConnectionError? {
        if let connectionError = error as? ProxyConnectionError { return connectionError }
        guard let connectionError = error as? NIOConnectionError,
              connectionError.connectionErrors.isEmpty else { return nil }
        for dnsError in [connectionError.dnsAError, connectionError.dnsAAAAError] {
            if let policyError = dnsError as? ProxyConnectionError, case .egressBlocked = policyError {
                return policyError
            }
        }
        return nil
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

import Foundation
import NIOQUIC
import SwiftMITM

extension HTTP3Transport {
    func emitFailure(id: UUID, target: CapturedTarget?, error: HTTP3ProxyError, downstream: Bool = false) {
        let reason: CapturedConnectionFailureReason
        let stage: ConnectionFailureStage
        let code: ConnectionFailureCode
        switch error {
        case .resourceLimit:
            (reason, stage, code) = (.transportFailure, .admission, .resourceLimit)
        case .egressDenied:
            (reason, stage, code) = (.destinationUnavailable, .routing, .egressDenied)
        case .invalidRoute, .destinationUnavailable:
            (reason, stage, code) = (.destinationUnavailable, .routing, .destinationUnavailable)
        case .upstreamConnectionFailed:
            (reason, stage, code) = (.upstreamConnectionFailed, .connect, .upstreamConnectFailed)
        case .tlsHandshakeFailed:
            (reason, stage, code) = (
                .tlsHandshakeFailed,
                downstream ? .downstreamTLS : .upstreamTLS,
                .tlsHandshakeFailed
            )
        case .handshakeTimedOut:
            (reason, stage, code) = (.timedOut, downstream ? .downstreamTLS : .upstreamTLS, .setupTimedOut)
        case .unsupportedProtocol:
            (reason, stage, code) = (.unsupportedProxyTransport, .negotiation, .unsupportedProtocol)
        case .invalidConfiguration, .protocolViolation, .shutdownTimedOut:
            (reason, stage, code) = (.transportFailure, .negotiation, .transportFailure)
        }
        sink.receive(.connectionFailure(.init(
            id: id,
            timestamp: Date(),
            reason: reason,
            target: target,
            stage: stage,
            code: code
        )))
    }
}

extension HTTP3ProxyError {
    static func fromTransport(_ error: any Error) -> Self? {
        if error is QUICStreamStorageAdmissionError {
            return .resourceLimit
        }
        return error as? Self
    }
}

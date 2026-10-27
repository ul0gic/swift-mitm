import Foundation
import NIOCore
import SwiftMITM

struct HTTP3DownstreamConnection {
    let id: UUID
    let channel: any Channel
    let target: CapturedTarget
}

enum HTTP3OriginSelection: Sendable {
    case fixed
    case mapped(HTTPAuthority)
}

struct HTTP3OriginStream {
    let sessionID: UUID
    let channel: any Channel
    let target: CapturedTarget
}

enum HTTP3ExchangeCancellation: Sendable {
    case cancelled
    case transportFailure(HTTP3ProxyError)
}

enum HTTP3StorageKind: Sendable {
    case nativeStream
    case parser
    case forwarding
    case capture
}

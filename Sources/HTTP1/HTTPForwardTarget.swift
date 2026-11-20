import Foundation
import NIOCore
import NIOHTTP1

struct HTTPForwardConnection: Sendable {
    let channel: Channel
    let target: CapturedTarget
    var releaseBufferedReads: (@Sendable () -> EventLoopFuture<Void>)?
    var usesHTTP2Codec = false
}

struct HTTPForwardTarget: Sendable {
    let connectionTarget: ConnectionTarget
    let requestTarget: String
    let scheme: String

    var host: String { connectionTarget.connectionHost }
    var port: Int { connectionTarget.port }
    var authority: String { connectionTarget.logicalAuthority }

    init?(scheme: String, authority: String, requestTarget: String) {
        guard let connectionTarget = ConnectionTarget(
            explicitConnectAuthority: authority,
            ingressProvenance: .explicitHTTP
        ) else { return nil }
        self.init(scheme: scheme, connectionTarget: connectionTarget, requestTarget: requestTarget)
    }

    init?(scheme: String, connectionTarget: ConnectionTarget, requestTarget: String) {
        guard ["http", "https"].contains(scheme),
              requestTarget.utf8.allSatisfy({ $0 > 0x20 && $0 < 0x7f }),
              requestTarget.hasPrefix("/") || requestTarget == "*" else { return nil }
        self.connectionTarget = connectionTarget
        self.scheme = scheme
        self.requestTarget = requestTarget
    }

    init?(head: HTTPRequestHead, scheme: String = "http") {
        let uri = head.uri
        guard uri.utf8.allSatisfy({ $0 > 0x20 && $0 < 0x7F }),
              !uri.contains("#"), let separator = uri.range(of: "://"),
              ["http", "https"].contains(scheme),
              uri[..<separator.lowerBound].lowercased() == scheme else { return nil }
        let remainder = uri[separator.upperBound...]
        let authorityEnd = remainder.firstIndex { $0 == "/" || $0 == "?" } ?? remainder.endIndex
        let rawAuthority = String(remainder[..<authorityEnd])
        guard !rawAuthority.isEmpty, !rawAuthority.contains("@") else { return nil }
        let explicitPort: Bool
        if rawAuthority.hasPrefix("[") {
            guard let bracket = rawAuthority.firstIndex(of: "]") else { return nil }
            explicitPort = rawAuthority.index(after: bracket) != rawAuthority.endIndex
        } else {
            explicitPort = rawAuthority.contains(":")
        }
        let connectAuthority = explicitPort ? rawAuthority : rawAuthority + (scheme == "https" ? ":443" : ":80")
        guard let target = ConnectionTarget(
            explicitConnectAuthority: connectAuthority,
            ingressProvenance: .explicitHTTP
        ) else { return nil }
        connectionTarget = target
        self.scheme = scheme
        let suffix = String(remainder[authorityEnd...])
        if suffix.isEmpty {
            requestTarget = head.method == .OPTIONS ? "*" : "/"
        } else {
            requestTarget = suffix.hasPrefix("?") ? "/" + suffix : suffix
        }
    }
}

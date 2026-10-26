import Foundation
import NIOCore
import NIOHTTP1

struct HTTPForwardConnection: Sendable {
    let channel: Channel
    let target: CapturedTarget
}

struct HTTPForwardTarget: Sendable {
    let host: String
    let port: Int
    let authority: String
    let requestTarget: String
    let scheme: String

    init?(head: HTTPRequestHead) {
        let uri = head.uri
        guard uri.utf8.allSatisfy({ $0 > 0x20 && $0 < 0x7F }),
            !uri.contains("#"), let separator = uri.range(of: "://"),
            uri[..<separator.lowerBound].lowercased() == "http" else { return nil }
        let remainder = uri[separator.upperBound...]
        let authorityEnd = remainder.firstIndex(where: { $0 == "/" || $0 == "?" }) ?? remainder.endIndex
        let rawAuthority = String(remainder[..<authorityEnd])
        guard !rawAuthority.isEmpty, !rawAuthority.contains("@") else { return nil }
        let explicitPort: Bool
        if rawAuthority.hasPrefix("[") {
            guard let bracket = rawAuthority.firstIndex(of: "]") else { return nil }
            explicitPort = rawAuthority.index(after: bracket) != rawAuthority.endIndex
        } else {
            explicitPort = rawAuthority.contains(":")
        }
        let connectAuthority = explicitPort ? rawAuthority : rawAuthority + ":80"
        guard let target = ConnectionTarget(
            explicitConnectAuthority: connectAuthority,
            ingressProvenance: .explicitHTTP
        ) else { return nil }
        host = target.connectionHost
        port = target.port
        authority = target.logicalAuthority
        scheme = "http"
        let suffix = String(remainder[authorityEnd...])
        if suffix.isEmpty {
            requestTarget = head.method == .OPTIONS ? "*" : "/"
        } else {
            requestTarget = suffix.hasPrefix("?") ? "/" + suffix : suffix
        }
    }
}

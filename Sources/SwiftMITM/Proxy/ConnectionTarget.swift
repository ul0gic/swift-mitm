import NIOCore

enum ConnectionIngressProvenance: Sendable {
    case explicitConnect
    case trustedProxyV2(originalClient: SocketAddress)
}

struct ConnectionTarget: Sendable {
    let connectionHost: String
    let port: Int
    let logicalAuthority: String
    let tlsServerName: String?
    let leafIdentity: String
    let ingressProvenance: ConnectionIngressProvenance

    init?(explicitConnectAuthority authority: String) {
        guard let separator = authority.lastIndex(of: ":") else { return nil }
        var host = String(authority[authority.startIndex ..< separator])
        let portText = authority[authority.index(after: separator)...]
        guard let port = Int(portText), (1...65535).contains(port), !host.isEmpty else { return nil }
        if host.hasPrefix("["), host.hasSuffix("]") {
            host = String(host.dropFirst().dropLast())
        }
        connectionHost = host
        self.port = port
        logicalAuthority = "\(host):\(port)"
        tlsServerName = Self.isIPAddress(host) ? nil : host
        leafIdentity = host
        ingressProvenance = .explicitConnect
    }

    init?(proxyV2Metadata metadata: ProxyV2Metadata) {
        guard let host = metadata.destinationAddress.ipAddress,
            let port = metadata.destinationAddress.port,
            (1...65535).contains(port) else {
            return nil
        }
        connectionHost = host
        self.port = port
        logicalAuthority = Self.authority(host: host, port: port)
        tlsServerName = nil
        leafIdentity = host
        ingressProvenance = .trustedProxyV2(originalClient: metadata.sourceAddress)
    }

    func applyingTLSMetadata(_ metadata: ClientHelloMetadata) -> ConnectionTarget {
        guard let serverName = metadata.serverName else { return self }
        return ConnectionTarget(
            connectionHost: connectionHost,
            port: port,
            logicalAuthority: logicalAuthority,
            tlsServerName: serverName,
            leafIdentity: serverName,
            ingressProvenance: ingressProvenance
        )
    }

    private init(
        connectionHost: String,
        port: Int,
        logicalAuthority: String,
        tlsServerName: String?,
        leafIdentity: String,
        ingressProvenance: ConnectionIngressProvenance
    ) {
        self.connectionHost = connectionHost
        self.port = port
        self.logicalAuthority = logicalAuthority
        self.tlsServerName = tlsServerName
        self.leafIdentity = leafIdentity
        self.ingressProvenance = ingressProvenance
    }

    private static func isIPAddress(_ host: String) -> Bool {
        (try? SocketAddress(ipAddress: host, port: 0)) != nil
    }

    private static func authority(host: String, port: Int) -> String {
        host.contains(":") ? "[\(host)]:\(port)" : "\(host):\(port)"
    }
}

struct ResolvedTarget: Sendable {
    let unresolved: ConnectionTarget
    let connectedAddress: SocketAddress
}

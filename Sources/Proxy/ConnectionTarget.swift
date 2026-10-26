import NIOCore

enum ConnectionIngressProvenance: Sendable {
    case explicitConnect
    case explicitHTTP
    case priorKnowledgeHTTP2
    case trustedProxyV2(originalClient: SocketAddress)
}

struct ConnectionTarget: Sendable {
    let connectionHost: String
    let port: Int
    let logicalAuthority: String
    let tlsServerName: String?
    let leafIdentity: String
    let ingressProvenance: ConnectionIngressProvenance

    init?(
        explicitConnectAuthority authority: String,
        ingressProvenance: ConnectionIngressProvenance = .explicitConnect
    ) {
        guard let separator = authority.lastIndex(of: ":") else { return nil }
        var host = String(authority[authority.startIndex ..< separator])
        let portText = authority[authority.index(after: separator)...]
        guard !portText.isEmpty, portText.utf8.allSatisfy({ (48...57).contains($0) }),
              let port = Int(portText) else { return nil }
        if host.hasPrefix("["), host.hasSuffix("]") {
            host = String(host.dropFirst().dropLast())
            guard host.contains(":"), Self.isIPAddress(host) else { return nil }
        } else if host.contains(":") {
            return nil
        }
        self.init(host: host, port: port, ingressProvenance: ingressProvenance)
    }

    init?(host: String, port: Int, ingressProvenance: ConnectionIngressProvenance) {
        guard (1...65535).contains(port), let host = Self.normalizedHost(host) else { return nil }
        connectionHost = host
        self.port = port
        logicalAuthority = Self.authority(host: host, port: port)
        tlsServerName = Self.isIPAddress(host) ? nil : host
        leafIdentity = host
        self.ingressProvenance = ingressProvenance
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

    static func normalizedHost(_ host: String) -> String? {
        guard !host.isEmpty, !host.contains("%") else { return nil }
        if let address = try? SocketAddress(ipAddress: host, port: 0) {
            return address.ipAddress
        }
        let normalized = host.hasSuffix(".") ? String(host.dropLast()).lowercased() : host.lowercased()
        guard !normalized.isEmpty, normalized.utf8.count <= 253 else { return nil }
        let labels = normalized.split(separator: ".", omittingEmptySubsequences: false)
        guard labels.allSatisfy({ label in
            !label.isEmpty && label.utf8.count <= 63 && label.first != "-" && label.last != "-"
                && label.utf8.allSatisfy { (97...122).contains($0) || (48...57).contains($0) || $0 == 45 }
        }) else { return nil }
        return normalized
    }

    private static func authority(host: String, port: Int) -> String {
        host.contains(":") ? "[\(host)]:\(port)" : "\(host):\(port)"
    }
}

struct ResolvedTarget: Sendable {
    let unresolved: ConnectionTarget
    let connectedAddress: SocketAddress
}

import NIOCore

public enum ProxyIngress: Sendable {
    case explicitConnect
    case trustedProxyV2(TrustedProxyV2Ingress)
}

public struct TrustedPeerPolicy: Sendable {
    private enum Network: Sendable {
        case ipv4(bytes: [UInt8], prefixLength: Int)
        case ipv6(bytes: [UInt8], prefixLength: Int)

        func contains(_ address: SocketAddress) -> Bool {
            switch (self, address) {
            case let (.ipv4(network, prefixLength), .v4(peer)):
                let bytes = withUnsafeBytes(of: peer.address.sin_addr) { Array($0) }
                return Self.hasPrefix(bytes, network: network, prefixLength: prefixLength)
            case let (.ipv4(network, prefixLength), .v6(peer)):
                let bytes = withUnsafeBytes(of: peer.address.sin6_addr) { Array($0) }
                guard bytes.prefix(12).elementsEqual([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF]) else {
                    return false
                }
                return Self.hasPrefix(Array(bytes.suffix(4)), network: network, prefixLength: prefixLength)
            case let (.ipv6(network, prefixLength), .v6(peer)):
                let bytes = withUnsafeBytes(of: peer.address.sin6_addr) { Array($0) }
                return Self.hasPrefix(bytes, network: network, prefixLength: prefixLength)
            case (.ipv4, .unixDomainSocket), (.ipv6, .v4), (.ipv6, .unixDomainSocket):
                return false
            }
        }

        private static func hasPrefix(_ bytes: [UInt8], network: [UInt8], prefixLength: Int) -> Bool {
            let wholeBytes = prefixLength / 8
            guard bytes.prefix(wholeBytes).elementsEqual(network.prefix(wholeBytes)) else { return false }
            let remainingBits = prefixLength % 8
            guard remainingBits > 0 else { return true }
            let mask = UInt8.max << (8 - remainingBits)
            return bytes[wholeBytes] & mask == network[wholeBytes] & mask
        }
    }

    public let addressesAndCIDRs: [String]
    private let networks: [Network]

    public init?(addressesAndCIDRs: [String]) {
        guard !addressesAndCIDRs.isEmpty else { return nil }
        var parsedNetworks: [Network] = []
        parsedNetworks.reserveCapacity(addressesAndCIDRs.count)
        for entry in addressesAndCIDRs {
            guard let network = Self.parse(entry) else { return nil }
            parsedNetworks.append(network)
        }
        self.addressesAndCIDRs = addressesAndCIDRs
        networks = parsedNetworks
    }

    public static let loopback = TrustedPeerPolicy(
        addressesAndCIDRs: ["127.0.0.0/8", "::1/128"],
        networks: [
            .ipv4(bytes: [127, 0, 0, 0], prefixLength: 8),
            .ipv6(bytes: Array(repeating: 0, count: 15) + [1], prefixLength: 128)
        ]
    )

    func admits(_ address: SocketAddress) -> Bool {
        networks.contains { $0.contains(address) }
    }

    private static func parse(_ entry: String) -> Network? {
        guard !entry.isEmpty, !entry.contains(where: \.isWhitespace), !entry.contains("%") else { return nil }
        let components = entry.split(separator: "/", omittingEmptySubsequences: false)
        guard components.count <= 2 else { return nil }
        let addressText = String(components[0])
        guard let address = try? SocketAddress(ipAddress: addressText, port: 0) else { return nil }
        switch address {
        case .v4(let address):
            guard let prefixLength = parsePrefixLength(components, maximum: 32) else { return nil }
            let bytes = withUnsafeBytes(of: address.address.sin_addr) { Array($0) }
            return .ipv4(bytes: bytes, prefixLength: prefixLength)
        case .v6(let address):
            guard let prefixLength = parsePrefixLength(components, maximum: 128) else { return nil }
            let bytes = withUnsafeBytes(of: address.address.sin6_addr) { Array($0) }
            return .ipv6(bytes: bytes, prefixLength: prefixLength)
        case .unixDomainSocket:
            return nil
        }
    }

    private static func parsePrefixLength(_ components: [Substring], maximum: Int) -> Int? {
        guard components.count == 2 else { return maximum }
        let text = components[1]
        guard !text.isEmpty, text.utf8.allSatisfy({ (48...57).contains($0) }),
            let prefixLength = Int(text), (0...maximum).contains(prefixLength) else {
            return nil
        }
        return prefixLength
    }

    private init(addressesAndCIDRs: [String], networks: [Network]) {
        self.addressesAndCIDRs = addressesAndCIDRs
        self.networks = networks
    }
}

public struct TrustedProxyV2Ingress: Sendable {
    public static let defaultProxyHeaderMaximumBytes = 4_096
    public static let defaultProxyHeaderDeadline = Duration.seconds(5)
    public static let defaultClassificationMaximumBytes = 65_536
    public static let defaultClassificationDeadline = Duration.seconds(1)

    public let trustedPeers: TrustedPeerPolicy
    public let proxyHeaderMaximumBytes: Int
    public let proxyHeaderDeadline: Duration
    public let classificationMaximumBytes: Int
    public let classificationDeadline: Duration

    public init?(
        trustedPeers: TrustedPeerPolicy,
        proxyHeaderMaximumBytes: Int = defaultProxyHeaderMaximumBytes,
        proxyHeaderDeadline: Duration = defaultProxyHeaderDeadline,
        classificationMaximumBytes: Int = defaultClassificationMaximumBytes,
        classificationDeadline: Duration = defaultClassificationDeadline
    ) {
        guard (16...(16 + Int(UInt16.max))).contains(proxyHeaderMaximumBytes),
            (1...Self.defaultClassificationMaximumBytes).contains(classificationMaximumBytes),
            proxyHeaderDeadline.nioTimeAmount != nil,
            classificationDeadline.nioTimeAmount != nil else {
            return nil
        }
        self.trustedPeers = trustedPeers
        self.proxyHeaderMaximumBytes = proxyHeaderMaximumBytes
        self.proxyHeaderDeadline = proxyHeaderDeadline
        self.classificationMaximumBytes = classificationMaximumBytes
        self.classificationDeadline = classificationDeadline
    }

    var proxyHeaderDeadlineTimeAmount: TimeAmount {
        proxyHeaderDeadline.nioTimeAmount ?? .nanoseconds(1)
    }

    var classificationDeadlineTimeAmount: TimeAmount {
        classificationDeadline.nioTimeAmount ?? .nanoseconds(1)
    }
}

public struct ProxyTimeoutPolicy: Sendable {
    public static let defaultUpstreamConnectDeadline = Duration.seconds(10)
    public static let defaultTLSHandshakeDeadline = Duration.seconds(10)
    public static let defaultHTTP2InitialSettingsDeadline = Duration.seconds(5)

    public let upstreamConnectDeadline: Duration
    public let tlsHandshakeDeadline: Duration
    public let http2InitialSettingsDeadline: Duration

    public static let `default` = ProxyTimeoutPolicy(
        upstreamConnectDeadline: defaultUpstreamConnectDeadline,
        tlsHandshakeDeadline: defaultTLSHandshakeDeadline,
        http2InitialSettingsDeadline: defaultHTTP2InitialSettingsDeadline,
        validated: ()
    )

    public init?(
        upstreamConnectDeadline: Duration = defaultUpstreamConnectDeadline,
        tlsHandshakeDeadline: Duration = defaultTLSHandshakeDeadline,
        http2InitialSettingsDeadline: Duration = defaultHTTP2InitialSettingsDeadline
    ) {
        guard upstreamConnectDeadline.nioTimeAmount != nil,
            tlsHandshakeDeadline.nioTimeAmount != nil,
            http2InitialSettingsDeadline.nioTimeAmount != nil else {
            return nil
        }
        self.upstreamConnectDeadline = upstreamConnectDeadline
        self.tlsHandshakeDeadline = tlsHandshakeDeadline
        self.http2InitialSettingsDeadline = http2InitialSettingsDeadline
    }

    private init(
        upstreamConnectDeadline: Duration,
        tlsHandshakeDeadline: Duration,
        http2InitialSettingsDeadline: Duration,
        validated _: Void
    ) {
        self.upstreamConnectDeadline = upstreamConnectDeadline
        self.tlsHandshakeDeadline = tlsHandshakeDeadline
        self.http2InitialSettingsDeadline = http2InitialSettingsDeadline
    }
}

extension Duration {
    var nioTimeAmount: TimeAmount? {
        let durationComponents = components
        guard durationComponents.seconds >= 0, durationComponents.attoseconds >= 0 else { return nil }
        let (secondNanoseconds, secondsOverflow) = durationComponents.seconds.multipliedReportingOverflow(
            by: 1_000_000_000
        )
        guard !secondsOverflow else { return nil }
        let attosecondNanoseconds = durationComponents.attoseconds / 1_000_000_000
        let (nanoseconds, additionOverflow) = secondNanoseconds.addingReportingOverflow(attosecondNanoseconds)
        guard !additionOverflow, nanoseconds > 0 else { return nil }
        return .nanoseconds(nanoseconds)
    }
}

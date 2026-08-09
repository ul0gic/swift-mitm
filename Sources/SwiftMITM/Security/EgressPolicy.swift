import NIOCore

public struct EgressPolicy: Sendable {
    private static let blockedIPv4Prefixes: [([UInt8], Int)] = [
        ([0], 8), ([10], 8), ([100, 64], 10), ([127], 8), ([169, 254], 16), ([172, 16], 12),
        ([192, 0, 0], 24), ([192, 0, 2], 24), ([192, 88, 99], 24), ([192, 168], 16),
        ([198, 18], 15), ([198, 51, 100], 24), ([203, 0, 113], 24), ([224], 3)
    ]
    private static let blockedIPv6Prefixes: [([UInt8], Int)] = [
        ([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 128),
        ([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 128),
        ([0x00, 0x64, 0xff, 0x9b, 0, 1], 48), ([0x01, 0x00, 0, 0, 0, 0, 0, 0], 64),
        ([0x01, 0x00, 0, 0, 0, 0, 0, 1], 64), ([0x20, 0x01, 0x0d, 0xb8], 32),
        ([0x20, 0x02], 16), ([0x3f, 0xff, 0], 20), ([0x5f, 0x00], 16), ([0xfc], 7),
        ([0xfe, 0x80], 10), ([0xfe, 0xc0], 10), ([0xff], 8)
    ]
    public var allowInternal: Bool

    public init(allowInternal: Bool = false) {
        self.allowInternal = allowInternal
    }

    public static let `default` = EgressPolicy()

    func denies(_ address: SocketAddress) -> Bool {
        guard !allowInternal else { return false }
        return Self.isInternal(address)
    }

    func deniesLiteral(_ host: String) -> Bool {
        guard !allowInternal, let address = try? SocketAddress(ipAddress: host, port: 0) else { return false }
        return Self.isInternal(address)
    }

    static func isInternal(_ address: SocketAddress) -> Bool {
        switch address {
        case .v4(let address):
            return isInternalV4(withUnsafeBytes(of: address.address.sin_addr) { Array($0) })
        case .v6(let address):
            return isInternalV6(withUnsafeBytes(of: address.address.sin6_addr) { Array($0) })
        case .unixDomainSocket:
            return true
        }
    }

    static func isLoopback(_ address: SocketAddress) -> Bool {
        switch address {
        case .v4(let address):
            return (withUnsafeBytes(of: address.address.sin_addr) { Array($0) }).first == 127
        case .v6(let address):
            let bytes = withUnsafeBytes(of: address.address.sin6_addr) { Array($0) }
            return bytes.dropLast().allSatisfy { $0 == 0 } && bytes.last == 1
        case .unixDomainSocket:
            return false
        }
    }

    private static func isInternalV4(_ octets: [UInt8]) -> Bool {
        guard octets.count == 4 else { return true }
        if octets == [192, 0, 0, 9] || octets == [192, 0, 0, 10] {
            return false
        }
        return matchesAnyPrefix(octets, prefixes: blockedIPv4Prefixes)
    }

    private static func isInternalV6(_ bytes: [UInt8]) -> Bool {
        guard bytes.count == 16 else { return true }
        if hasPrefix(bytes, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff], bits: 96) {
            return isInternalV4(Array(bytes[12..<16]))
        }
        if hasPrefix(bytes, [0x00, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0], bits: 96) {
            return isInternalV4(Array(bytes[12..<16]))
        }
        if hasPrefix(bytes, [0x20, 0x01, 0], bits: 23) {
            return !isGloballyReachableIETFAddress(bytes)
        }
        return matchesAnyPrefix(bytes, prefixes: blockedIPv6Prefixes) || !hasPrefix(bytes, [0x20], bits: 3)
    }

    private static func isGloballyReachableIETFAddress(_ bytes: [UInt8]) -> Bool {
        let anycasts: [[UInt8]] = [
            [0x20, 0x01, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            [0x20, 0x01, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
            [0x20, 0x01, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]
        ]
        if anycasts.contains(bytes) {
            return true
        }
        if hasPrefix(bytes, [0x20, 0x01, 0, 3], bits: 32) {
            return true
        }
        if hasPrefix(bytes, [0x20, 0x01, 0, 4, 0x01, 0x12], bits: 48) {
            return true
        }
        if hasPrefix(bytes, [0x20, 0x01, 0, 0x20], bits: 28) {
            return true
        }
        return hasPrefix(bytes, [0x20, 0x01, 0, 0x30], bits: 28)
    }

    private static func hasPrefix(_ bytes: [UInt8], _ prefix: [UInt8], bits: Int) -> Bool {
        let wholeBytes = bits / 8
        guard bytes.count >= wholeBytes, prefix.count >= wholeBytes else { return false }
        if !bytes.prefix(wholeBytes).elementsEqual(prefix.prefix(wholeBytes)) {
            return false
        }
        let remainingBits = bits % 8
        guard remainingBits > 0 else { return true }
        guard bytes.count > wholeBytes, prefix.count > wholeBytes else { return false }
        let mask = UInt8.max << (8 - remainingBits)
        return bytes[wholeBytes] & mask == prefix[wholeBytes] & mask
    }

    private static func matchesAnyPrefix(_ bytes: [UInt8], prefixes: [([UInt8], Int)]) -> Bool {
        prefixes.contains { hasPrefix(bytes, $0.0, bits: $0.1) }
    }
}

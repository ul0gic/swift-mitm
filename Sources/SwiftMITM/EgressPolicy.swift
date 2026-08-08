import NIOCore

/// Decides whether the proxy may dial a resolved upstream address. Default denies internal ranges
/// (loopback, link-local incl. cloud metadata, RFC1918, ULA) so a page driven through the proxy can't reach them.
public struct EgressPolicy: Sendable {
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
        case .v4(let a):
            return isInternalV4(withUnsafeBytes(of: a.address.sin_addr) { Array($0) })
        case .v6(let a):
            return isInternalV6(withUnsafeBytes(of: a.address.sin6_addr) { Array($0) })
        case .unixDomainSocket:
            return true
        }
    }

    static func isLoopback(_ address: SocketAddress) -> Bool {
        switch address {
        case .v4(let a):
            return (withUnsafeBytes(of: a.address.sin_addr) { Array($0) }).first == 127
        case .v6(let a):
            let bytes = withUnsafeBytes(of: a.address.sin6_addr) { Array($0) }
            return bytes.dropLast().allSatisfy { $0 == 0 } && bytes.last == 1
        case .unixDomainSocket:
            return false
        }
    }

    private static func isInternalV4(_ octets: [UInt8]) -> Bool {
        guard octets.count == 4 else { return true }
        switch octets[0] {
        case 0, 10, 127: return true
        case 169: return octets[1] == 254
        case 172: return (16...31).contains(octets[1])
        case 192: return octets[1] == 168
        default: return false
        }
    }

    private static func isInternalV6(_ bytes: [UInt8]) -> Bool {
        guard bytes.count == 16 else { return true }
        if bytes.allSatisfy({ $0 == 0 }) { return true }
        if bytes.dropLast().allSatisfy({ $0 == 0 }) && bytes.last == 1 { return true }
        if bytes[0] == 0xfe && (bytes[1] & 0xc0) == 0x80 { return true }
        if (bytes[0] & 0xfe) == 0xfc { return true }
        if bytes[0..<10].allSatisfy({ $0 == 0 }) && bytes[10] == 0xff && bytes[11] == 0xff {
            return isInternalV4(Array(bytes[12..<16]))
        }
        return false
    }
}

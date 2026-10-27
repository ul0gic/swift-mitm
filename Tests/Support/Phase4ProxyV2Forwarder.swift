struct Phase4ProxyV2Header: Equatable, Sendable {
    static let signature = Phase2ProxyV2Corpus.signature

    let bytes: [UInt8]

    static func ipv4(
        source: [UInt8],
        destination: [UInt8],
        sourcePort: Int,
        destinationPort: Int,
        tlvs: [UInt8] = []
    ) -> Phase4ProxyV2Header {
        precondition(source.count == 4 && destination.count == 4)
        let address = source + destination + encodedPort(sourcePort) + encodedPort(destinationPort) + tlvs
        return .init(bytes: signature + [0x21, 0x11] + encodedLength(address.count) + address)
    }

    static func ipv6(
        source: [UInt8],
        destination: [UInt8],
        sourcePort: Int,
        destinationPort: Int,
        tlvs: [UInt8] = []
    ) -> Phase4ProxyV2Header {
        precondition(source.count == 16 && destination.count == 16)
        let address = source + destination + encodedPort(sourcePort) + encodedPort(destinationPort) + tlvs
        return .init(bytes: signature + [0x21, 0x21] + encodedLength(address.count) + address)
    }

    private static func encodedPort(_ port: Int) -> [UInt8] {
        precondition((1 ... 65_535).contains(port))
        return [UInt8((port >> 8) & 0xFF), UInt8(port & 0xFF)]
    }

    private static func encodedLength(_ length: Int) -> [UInt8] {
        [UInt8((length >> 8) & 0xFF), UInt8(length & 0xFF)]
    }
}

import Foundation

struct Phase2ProxyV2Vector: Sendable {
    let name: String
    let bytes: [UInt8]
}

enum Phase2ProxyV2Corpus {
    static let signature: [UInt8] = [
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51,
        0x55, 0x49, 0x54, 0x0A
    ]
    static let replay: [UInt8] = [0x16, 0x03, 0x03, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04]

    static let ipv4 = Phase2ProxyV2Vector(
        name: "ipv4-tcp-with-alpn-tlv",
        bytes: signature + [
            0x21, 0x11, 0x00, 0x11,
            0xC0, 0x00, 0x02, 0x0A,
            0xC6, 0x33, 0x64, 0x14,
            0x30, 0x39, 0x01, 0xBB,
            0x01, 0x00, 0x02, 0x68, 0x32
        ]
    )

    static let ipv6 = Phase2ProxyV2Vector(
        name: "ipv6-tcp",
        bytes: signature + [
            0x21, 0x21, 0x00, 0x24,
            0x20, 0x01, 0x0D, 0xB8, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x20, 0x01, 0x0D, 0xB8, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
            0x1F, 0x90, 0x20, 0xFB
        ]
    )

    static let malformed: [Phase2ProxyV2Vector] = [
        .init(name: "v1-text", bytes: Array("PROXY TCP4 192.0.2.1 198.51.100.1 12345 443\r\n".utf8)),
        .init(name: "bad-signature", bytes: [0x0D, 0x0A, 0x0D, 0x0B]),
        .init(name: "truncated-fixed-header", bytes: Array(ipv4.bytes.prefix(15))),
        .init(name: "version-one", bytes: replacing(ipv4.bytes, at: 12, with: 0x11)),
        .init(name: "local-command", bytes: replacing(ipv4.bytes, at: 12, with: 0x20)),
        .init(name: "unspecified-family", bytes: replacing(ipv4.bytes, at: 13, with: 0x01)),
        .init(name: "datagram-transport", bytes: replacing(ipv4.bytes, at: 13, with: 0x12)),
        .init(name: "truncated-address", bytes: Array(ipv4.bytes.dropLast())),
        .init(
            name: "declared-oversize",
            bytes: replacing(replacing(ipv4.bytes, at: 14, with: 0xFF), at: 15, with: 0xFF)
        ),
        .init(name: "malformed-tlv-length", bytes: replacing(ipv4.bytes, at: 31, with: 0x03))
    ]

    private static func replacing(_ bytes: [UInt8], at index: Int, with byte: UInt8) -> [UInt8] {
        var copy = bytes
        copy[index] = byte
        return copy
    }
}

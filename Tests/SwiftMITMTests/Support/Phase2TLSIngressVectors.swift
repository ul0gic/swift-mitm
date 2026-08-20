enum Phase2TLSIngressVectors {
    static let maximumVectorBytes = 4_096

    static let http11ClientHello = clientHello(hostname: "localhost", protocols: ["http/1.1"])
    static let http2ClientHello = clientHello(hostname: "localhost", protocols: ["h2", "http/1.1"])
    static let noSNIClientHello = clientHello(hostname: nil, protocols: ["h2"])

    static let malformed: [[UInt8]] = [
        [0x16, 0x03, 0x03, 0x00],
        [0x17, 0x03, 0x03, 0x00, 0x00],
        [0x16, 0x03, 0x03, 0x00, 0x04, 0x01, 0x01, 0x00, 0x00]
    ]

    private static func clientHello(hostname: String?, protocols: [String]) -> [UInt8] {
        var body: [UInt8] = [0x03, 0x03]
        body.append(contentsOf: (0 ..< 32).map(UInt8.init))
        body.append(0)
        body.append(contentsOf: [0x00, 0x02, 0x13, 0x01])
        body.append(contentsOf: [0x01, 0x00])

        var extensions: [UInt8] = []
        if let hostname {
            let name = Array(hostname.utf8)
            let serverName = [UInt8(0)] + encodedUInt16(name.count) + name
            let serverNames = encodedUInt16(serverName.count) + serverName
            extensions += [0x00, 0x00] + encodedUInt16(serverNames.count) + serverNames
        }
        let names = protocols.flatMap { value -> [UInt8] in
            let bytes = Array(value.utf8)
            return [UInt8(bytes.count)] + bytes
        }
        let alpn = encodedUInt16(names.count) + names
        extensions += [0x00, 0x10] + encodedUInt16(alpn.count) + alpn
        body += encodedUInt16(extensions.count) + extensions

        let handshake = [UInt8(0x01)] + encodedUInt24(body.count) + body
        return [0x16, 0x03, 0x03] + encodedUInt16(handshake.count) + handshake
    }

    private static func encodedUInt16(_ value: Int) -> [UInt8] {
        [UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }

    private static func encodedUInt24(_ value: Int) -> [UInt8] {
        [UInt8((value >> 16) & 0xFF), UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }
}

enum ClientHelloALPNParser {
    static let maximumClientHelloBytes = 65_536

    static func parse(_ bytes: [UInt8]) throws -> ClientALPNOffer? {
        var recordIndex = 0
        var handshake: [UInt8] = []
        while recordIndex + 5 <= bytes.count {
            guard bytes[recordIndex] == 22 else { throw ProxyALPNError.malformedClientHello }
            let recordLength = integer(bytes[recordIndex + 3], bytes[recordIndex + 4])
            let payloadIndex = recordIndex + 5
            guard recordLength <= maximumClientHelloBytes else {
                throw ProxyALPNError.malformedClientHello
            }
            guard payloadIndex + recordLength <= bytes.count else { return nil }
            handshake.append(contentsOf: bytes[payloadIndex ..< payloadIndex + recordLength])
            guard handshake.count <= maximumClientHelloBytes else {
                throw ProxyALPNError.malformedClientHello
            }
            if handshake.count >= 4 {
                guard handshake[0] == 1 else { throw ProxyALPNError.malformedClientHello }
                let handshakeLength = integer(handshake[1], handshake[2], handshake[3])
                guard handshakeLength <= maximumClientHelloBytes - 4 else {
                    throw ProxyALPNError.malformedClientHello
                }
                if handshake.count >= handshakeLength + 4 {
                    return try parseExtensions(Array(handshake[4 ..< handshakeLength + 4]))
                }
            }
            recordIndex = payloadIndex + recordLength
        }
        return nil
    }

    private static func parseExtensions(_ clientHello: [UInt8]) throws -> ClientALPNOffer {
        var index = 34
        try skipVector(lengthBytes: 1, bytes: clientHello, index: &index)
        try skipVector(lengthBytes: 2, bytes: clientHello, index: &index)
        try skipVector(lengthBytes: 1, bytes: clientHello, index: &index)
        guard index < clientHello.count else { return .absent }
        let extensionsLength = try readInteger(lengthBytes: 2, bytes: clientHello, index: &index)
        guard index + extensionsLength == clientHello.count else { throw ProxyALPNError.malformedClientHello }
        let extensionsEnd = index + extensionsLength
        while index < extensionsEnd {
            let type = try readInteger(lengthBytes: 2, bytes: clientHello, index: &index)
            let length = try readInteger(lengthBytes: 2, bytes: clientHello, index: &index)
            guard index + length <= extensionsEnd else { throw ProxyALPNError.malformedClientHello }
            if type == 16 {
                return try parseALPN(Array(clientHello[index ..< index + length]))
            }
            index += length
        }
        return .absent
    }

    private static func parseALPN(_ extensionBytes: [UInt8]) throws -> ClientALPNOffer {
        var index = 0
        let protocolsLength = try readInteger(lengthBytes: 2, bytes: extensionBytes, index: &index)
        guard protocolsLength > 0, index + protocolsLength == extensionBytes.count else {
            throw ProxyALPNError.malformedClientHello
        }
        var protocols: [ALPNProtocol] = []
        while index < extensionBytes.count {
            let length = try readInteger(lengthBytes: 1, bytes: extensionBytes, index: &index)
            guard length > 0, index + length <= extensionBytes.count else {
                throw ProxyALPNError.malformedClientHello
            }
            let name = String(bytes: extensionBytes[index ..< index + length], encoding: .utf8)
            if let name, let supported = ALPNProtocol(rawValue: name), !protocols.contains(supported) {
                protocols.append(supported)
            }
            index += length
        }
        return .protocols(protocols)
    }

    private static func skipVector(lengthBytes: Int, bytes: [UInt8], index: inout Int) throws {
        let length = try readInteger(lengthBytes: lengthBytes, bytes: bytes, index: &index)
        guard index + length <= bytes.count else { throw ProxyALPNError.malformedClientHello }
        index += length
    }

    private static func readInteger(lengthBytes: Int, bytes: [UInt8], index: inout Int) throws -> Int {
        guard lengthBytes > 0, index + lengthBytes <= bytes.count else {
            throw ProxyALPNError.malformedClientHello
        }
        let value = bytes[index ..< index + lengthBytes].reduce(0) { ($0 << 8) | Int($1) }
        index += lengthBytes
        return value
    }

    private static func integer(_ bytes: UInt8...) -> Int {
        bytes.reduce(0) { ($0 << 8) | Int($1) }
    }
}

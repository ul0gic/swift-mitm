import NIOCore

struct ALPNProtocolIdentifier: Equatable, Sendable {
    let bytes: [UInt8]
}

struct ClientHelloMetadata: Sendable {
    let offeredALPNProtocols: [ALPNProtocolIdentifier]
    let supportedALPNProtocols: [ALPNProtocol]
    let hasALPNExtension: Bool
    let serverName: String?
    let encryptedClientHelloDetected: Bool

    var compatibilityALPNOffer: ClientALPNOffer {
        hasALPNExtension ? .protocols(supportedALPNProtocols) : .absent
    }
}

enum ClientHelloInspectionBoundary {
    static let deadline = TimeAmount.seconds(1)
}

enum ClientHelloALPNParser {
    static let maximumClientHelloBytes = 65_536

    private static let serverNameExtension = 0
    private static let alpnExtension = 16
    private static let encryptedClientHelloExtension = 0xFE0D

    static func parse(_ bytes: [UInt8]) throws -> ClientALPNOffer? {
        try inspect(bytes)?.compatibilityALPNOffer
    }

    static func inspect(_ bytes: [UInt8]) throws -> ClientHelloMetadata? {
        guard bytes.count <= maximumClientHelloBytes else {
            throw ProxyALPNError.malformedClientHello
        }
        var recordIndex = 0
        var handshake: [UInt8] = []
        while recordIndex < bytes.count {
            guard bytes[recordIndex] == 22 else { throw ProxyALPNError.malformedClientHello }
            guard recordIndex + 5 <= bytes.count else { return nil }
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
            if let metadata = try inspectCompleteHandshake(handshake) {
                return metadata
            }
            recordIndex = payloadIndex + recordLength
        }
        return nil
    }

    private static func inspectCompleteHandshake(_ handshake: [UInt8]) throws -> ClientHelloMetadata? {
        guard handshake.count >= 4 else { return nil }
        guard handshake[0] == 1 else { throw ProxyALPNError.malformedClientHello }
        let handshakeLength = integer(handshake[1], handshake[2], handshake[3])
        guard handshakeLength <= maximumClientHelloBytes - 4 else {
            throw ProxyALPNError.malformedClientHello
        }
        guard handshake.count >= handshakeLength + 4 else { return nil }
        return try parseClientHello(handshake, startIndex: 4, endIndex: handshakeLength + 4)
    }

    private static func parseClientHello(
        _ bytes: [UInt8],
        startIndex: Int,
        endIndex: Int
    ) throws -> ClientHelloMetadata {
        var index = startIndex + 34
        try skipVector(lengthBytes: 1, bytes: bytes, index: &index, endIndex: endIndex)
        try skipVector(lengthBytes: 2, bytes: bytes, index: &index, endIndex: endIndex)
        try skipVector(lengthBytes: 1, bytes: bytes, index: &index, endIndex: endIndex)
        guard index < endIndex else {
            return ClientHelloMetadata(
                offeredALPNProtocols: [],
                supportedALPNProtocols: [],
                hasALPNExtension: false,
                serverName: nil,
                encryptedClientHelloDetected: false
            )
        }
        let extensionsLength = try readInteger(
            lengthBytes: 2,
            bytes: bytes,
            index: &index,
            endIndex: endIndex
        )
        guard index + extensionsLength == endIndex else { throw ProxyALPNError.malformedClientHello }
        return try parseExtensions(bytes, startIndex: index, endIndex: endIndex)
    }

    private static func parseExtensions(
        _ bytes: [UInt8],
        startIndex: Int,
        endIndex: Int
    ) throws -> ClientHelloMetadata {
        var index = startIndex
        var offeredALPNProtocols: [ALPNProtocolIdentifier] = []
        var supportedALPNProtocols: [ALPNProtocol] = []
        var hasALPNExtension = false
        var hasServerNameExtension = false
        var serverName: String?
        var encryptedClientHelloDetected = false
        while index < endIndex {
            let type = try readInteger(lengthBytes: 2, bytes: bytes, index: &index, endIndex: endIndex)
            let length = try readInteger(lengthBytes: 2, bytes: bytes, index: &index, endIndex: endIndex)
            let extensionEndIndex = index + length
            guard extensionEndIndex <= endIndex else { throw ProxyALPNError.malformedClientHello }
            switch type {
            case serverNameExtension:
                guard !hasServerNameExtension else { throw ProxyALPNError.malformedClientHello }
                hasServerNameExtension = true
                serverName = try parseServerName(bytes, startIndex: index, endIndex: extensionEndIndex)
            case alpnExtension:
                guard !hasALPNExtension else { throw ProxyALPNError.malformedClientHello }
                hasALPNExtension = true
                offeredALPNProtocols = try parseALPN(bytes, startIndex: index, endIndex: extensionEndIndex)
                supportedALPNProtocols = supportedProtocols(from: offeredALPNProtocols)
            case encryptedClientHelloExtension:
                guard !encryptedClientHelloDetected else { throw ProxyALPNError.malformedClientHello }
                encryptedClientHelloDetected = true
            default:
                break
            }
            index = extensionEndIndex
        }
        return ClientHelloMetadata(
            offeredALPNProtocols: offeredALPNProtocols,
            supportedALPNProtocols: supportedALPNProtocols,
            hasALPNExtension: hasALPNExtension,
            serverName: serverName,
            encryptedClientHelloDetected: encryptedClientHelloDetected
        )
    }

    private static func parseALPN(
        _ bytes: [UInt8],
        startIndex: Int,
        endIndex: Int
    ) throws -> [ALPNProtocolIdentifier] {
        var index = startIndex
        let protocolsLength = try readInteger(
            lengthBytes: 2,
            bytes: bytes,
            index: &index,
            endIndex: endIndex
        )
        guard protocolsLength > 0, index + protocolsLength == endIndex else {
            throw ProxyALPNError.malformedClientHello
        }
        var protocols: [ALPNProtocolIdentifier] = []
        while index < endIndex {
            let length = try readInteger(lengthBytes: 1, bytes: bytes, index: &index, endIndex: endIndex)
            guard length > 0, index + length <= endIndex else {
                throw ProxyALPNError.malformedClientHello
            }
            protocols.append(ALPNProtocolIdentifier(bytes: Array(bytes[index ..< index + length])))
            index += length
        }
        return protocols
    }

    private static func supportedProtocols(
        from offeredProtocols: [ALPNProtocolIdentifier]
    ) -> [ALPNProtocol] {
        offeredProtocols.reduce(into: []) { supported, identifier in
            let protocolName: ALPNProtocol?
            switch identifier.bytes {
            case [0x68, 0x32]:
                protocolName = .http2
            case [0x68, 0x74, 0x74, 0x70, 0x2F, 0x31, 0x2E, 0x31]:
                protocolName = .http11
            default:
                protocolName = nil
            }
            guard let protocolName, !supported.contains(protocolName) else { return }
            supported.append(protocolName)
        }
    }

    private static func parseServerName(
        _ bytes: [UInt8],
        startIndex: Int,
        endIndex: Int
    ) throws -> String? {
        var index = startIndex
        let listLength = try readInteger(
            lengthBytes: 2,
            bytes: bytes,
            index: &index,
            endIndex: endIndex
        )
        guard listLength > 0, index + listLength == endIndex else {
            throw ProxyALPNError.malformedClientHello
        }
        var serverName: String?
        while index < endIndex {
            let nameType = try readInteger(lengthBytes: 1, bytes: bytes, index: &index, endIndex: endIndex)
            let nameLength = try readInteger(lengthBytes: 2, bytes: bytes, index: &index, endIndex: endIndex)
            guard nameLength > 0, index + nameLength <= endIndex else {
                throw ProxyALPNError.malformedClientHello
            }
            if nameType == 0 {
                guard serverName == nil else { throw ProxyALPNError.malformedClientHello }
                serverName = try normalizedDNSName(bytes[index ..< index + nameLength])
            }
            index += nameLength
        }
        return serverName
    }

    private static func normalizedDNSName(_ bytes: ArraySlice<UInt8>) throws -> String {
        guard bytes.count <= 253, bytes.allSatisfy({ $0 < 0x80 }) else {
            throw ProxyALPNError.malformedClientHello
        }
        let labels = bytes.split(separator: 0x2E, omittingEmptySubsequences: false)
        guard !labels.isEmpty, !isIPv4Address(labels) else {
            throw ProxyALPNError.malformedClientHello
        }
        for label in labels {
            guard
                !label.isEmpty,
                label.count <= 63,
                label.first.map(isASCIILetterOrDigit) == true,
                label.last.map(isASCIILetterOrDigit) == true,
                label.allSatisfy({ isASCIILetterOrDigit($0) || $0 == 0x2D })
            else {
                throw ProxyALPNError.malformedClientHello
            }
        }
        guard let name = String(bytes: bytes, encoding: .utf8) else {
            throw ProxyALPNError.malformedClientHello
        }
        return name.lowercased()
    }

    private static func isIPv4Address(_ labels: [ArraySlice<UInt8>.SubSequence]) -> Bool {
        guard labels.count == 4 else { return false }
        return labels.allSatisfy { label in
            guard
                !label.isEmpty,
                label.count <= 3,
                label.allSatisfy({ 0x30 ... 0x39 ~= $0 })
            else { return false }
            return label.reduce(0) { ($0 * 10) + Int($1 - 0x30) } <= 255
        }
    }

    private static func isASCIILetterOrDigit(_ byte: UInt8) -> Bool {
        0x30 ... 0x39 ~= byte || 0x41 ... 0x5A ~= byte || 0x61 ... 0x7A ~= byte
    }

    private static func skipVector(
        lengthBytes: Int,
        bytes: [UInt8],
        index: inout Int,
        endIndex: Int
    ) throws {
        let length = try readInteger(lengthBytes: lengthBytes, bytes: bytes, index: &index, endIndex: endIndex)
        guard index + length <= endIndex else { throw ProxyALPNError.malformedClientHello }
        index += length
    }

    private static func readInteger(
        lengthBytes: Int,
        bytes: [UInt8],
        index: inout Int,
        endIndex: Int
    ) throws -> Int {
        guard lengthBytes > 0, index + lengthBytes <= endIndex else {
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

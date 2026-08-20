import Foundation
import NIOCore

enum Phase5ProxyV2AddressFamily: String, Codable, Hashable, Sendable {
    case ipv4
    case ipv6
}

struct Phase5ProxyV2Endpoint: Codable, Equatable, Sendable {
    let family: Phase5ProxyV2AddressFamily
    let address: String
    let port: Int

    init(family: Phase5ProxyV2AddressFamily, address: String, port: Int) {
        self.family = family
        self.address = address
        self.port = port
    }

    init?(socketAddress: SocketAddress) {
        guard let address = socketAddress.ipAddress, let port = socketAddress.port, port > 0 else { return nil }
        switch socketAddress {
        case .v4:
            family = .ipv4
        case .v6:
            family = .ipv6
        case .unixDomainSocket:
            return nil
        }
        self.address = address
        self.port = port
    }
}

enum Phase5ProxyV2VectorRole: String, Codable, Hashable, Sendable {
    case emitter
    case receiver
}

enum Phase5ProxyV2Disposition: String, Codable, Equatable, Sendable {
    case accept
    case reject
}

struct Phase5ProxyV2ConformanceVector: Codable, Sendable {
    let id: String
    let roles: [Phase5ProxyV2VectorRole]
    let disposition: Phase5ProxyV2Disposition
    let headerHex: String
    let source: Phase5ProxyV2Endpoint?
    let destination: Phase5ProxyV2Endpoint?
    let tlvHex: String?
    let tlvCount: Int?
    let applicationHex: String?
    let reason: String?

    var headerBytes: [UInt8] { get throws { try Phase5Hex.decode(headerHex) } }
    var tlvBytes: [UInt8] { get throws { try Phase5Hex.decode(tlvHex ?? "") } }
    var applicationBytes: [UInt8] { get throws { try Phase5Hex.decode(applicationHex ?? "") } }
}

struct Phase5ProxyV2ConformanceDocument: Codable, Sendable {
    let format: String
    let schemaVersion: Int
    let contractVersion: String
    let maximumHeaderBytes: Int
    let vectors: [Phase5ProxyV2ConformanceVector]

    static func load() throws -> Phase5ProxyV2ConformanceDocument {
        let file = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Conformance/ProxyV2/v1.json")
        let document = try JSONDecoder().decode(Self.self, from: Data(contentsOf: file))
        try document.validate()
        return document
    }

    private func validate() throws {
        guard format == "swiftmitm.proxy-v2-conformance",
              schemaVersion == 1,
              contractVersion == "1.0.0",
              maximumHeaderBytes == 4_096,
              !vectors.isEmpty else {
            throw Phase5ProxyV2ConformanceError.invalidDocument
        }
        guard Set(vectors.map(\.id)).count == vectors.count, vectors.allSatisfy({ !$0.id.isEmpty }) else {
            throw Phase5ProxyV2ConformanceError.invalidDocument
        }
        for vector in vectors {
            try validate(vector)
        }
        let emitterIDs = Set(vectors.filter { $0.roles.contains(.emitter) }.map(\.id))
        guard emitterIDs == ["tcp4-minimal", "tcp6-minimal"] else {
            throw Phase5ProxyV2ConformanceError.invalidDocument
        }
    }

    private func validate(_ vector: Phase5ProxyV2ConformanceVector) throws {
        guard Set(vector.roles).count == vector.roles.count, vector.roles.contains(.receiver) else {
            throw Phase5ProxyV2ConformanceError.invalidDocument
        }
        let header = try Phase5Hex.decode(vector.headerHex)
        guard !header.isEmpty else { throw Phase5ProxyV2ConformanceError.invalidDocument }
        switch vector.disposition {
        case .accept:
            guard let source = vector.source,
                  let destination = vector.destination,
                  let tlvCount = vector.tlvCount,
                  let applicationHex = vector.applicationHex,
                  vector.reason == nil,
                  !applicationHex.isEmpty else {
                throw Phase5ProxyV2ConformanceError.invalidDocument
            }
            let tlvs = try Phase5Hex.decode(vector.tlvHex ?? "")
            guard try Phase5ProxyV2Encoder.encode(source: source, destination: destination, tlvs: tlvs) == header,
                  try Phase5ProxyV2Encoder.tlvCount(tlvs) == tlvCount,
                  header.count <= maximumHeaderBytes else {
                throw Phase5ProxyV2ConformanceError.invalidDocument
            }
            _ = try Phase5Hex.decode(applicationHex)
        case .reject:
            guard vector.roles == [.receiver],
                  vector.source == nil,
                  vector.destination == nil,
                  vector.tlvHex == nil,
                  vector.tlvCount == nil,
                  vector.applicationHex == nil,
                  vector.reason?.isEmpty == false else {
                throw Phase5ProxyV2ConformanceError.invalidDocument
            }
        }
    }
}

enum Phase5ProxyV2Encoder {
    private static let signature: [UInt8] = [
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51,
        0x55, 0x49, 0x54, 0x0A
    ]

    static func encode(
        source: Phase5ProxyV2Endpoint,
        destination: Phase5ProxyV2Endpoint,
        tlvs: [UInt8] = []
    ) throws -> [UInt8] {
        guard source.family == destination.family,
              (1 ... Int(UInt16.max)).contains(source.port),
              (1 ... Int(UInt16.max)).contains(destination.port) else {
            throw Phase5ProxyV2ConformanceError.invalidEndpoint
        }
        let sourceBytes = try addressBytes(source)
        let destinationBytes = try addressBytes(destination)
        let address = sourceBytes + destinationBytes + portBytes(source.port) + portBytes(destination.port) + tlvs
        guard address.count <= Int(UInt16.max) else {
            throw Phase5ProxyV2ConformanceError.invalidDocument
        }
        let familyAndTransport: UInt8 = source.family == .ipv4 ? 0x11 : 0x21
        return signature + [0x21, familyAndTransport] + lengthBytes(address.count) + address
    }

    static func tlvCount(_ bytes: [UInt8]) throws -> Int {
        var index = 0
        var count = 0
        while index < bytes.count {
            guard bytes.count - index >= 3 else { throw Phase5ProxyV2ConformanceError.invalidTLV }
            let length = (Int(bytes[index + 1]) << 8) | Int(bytes[index + 2])
            index += 3 + length
            guard index <= bytes.count else { throw Phase5ProxyV2ConformanceError.invalidTLV }
            count += 1
        }
        return count
    }

    private static func addressBytes(_ endpoint: Phase5ProxyV2Endpoint) throws -> [UInt8] {
        let address: SocketAddress
        do {
            address = try SocketAddress(ipAddress: endpoint.address, port: endpoint.port)
        } catch {
            throw Phase5ProxyV2ConformanceError.invalidEndpoint
        }
        switch (endpoint.family, address) {
        case (.ipv4, .v4(let value)):
            return withUnsafeBytes(of: value.address.sin_addr) { Array($0) }
        case (.ipv6, .v6(let value)):
            return withUnsafeBytes(of: value.address.sin6_addr) { Array($0) }
        case (.ipv4, .v6), (.ipv4, .unixDomainSocket), (.ipv6, .v4), (.ipv6, .unixDomainSocket):
            throw Phase5ProxyV2ConformanceError.invalidEndpoint
        }
    }

    private static func portBytes(_ port: Int) -> [UInt8] {
        [UInt8((port >> 8) & 0xFF), UInt8(port & 0xFF)]
    }

    private static func lengthBytes(_ length: Int) -> [UInt8] {
        [UInt8((length >> 8) & 0xFF), UInt8(length & 0xFF)]
    }
}

enum Phase5Hex {
    static func decode(_ value: String) throws -> [UInt8] {
        guard value == value.lowercased(), value.count.isMultiple(of: 2), value.utf8.allSatisfy(isHex) else {
            throw Phase5ProxyV2ConformanceError.invalidHex
        }
        var bytes: [UInt8] = []
        bytes.reserveCapacity(value.count / 2)
        var index = value.startIndex
        while index < value.endIndex {
            let next = value.index(index, offsetBy: 2)
            guard let byte = UInt8(value[index ..< next], radix: 16) else {
                throw Phase5ProxyV2ConformanceError.invalidHex
            }
            bytes.append(byte)
            index = next
        }
        return bytes
    }

    private static func isHex(_ byte: UInt8) -> Bool {
        (48 ... 57).contains(byte) || (97 ... 102).contains(byte)
    }
}

enum Phase5ProxyV2ConformanceError: Error, Equatable {
    case invalidDocument
    case invalidEndpoint
    case invalidHex
    case invalidTLV
}

import HTTPTypes
import SwiftMITM

struct HTTP3HeaderSnapshot {
    let fields: [HTTPHeaderField]
    let lease: HTTP3StorageLease?

    init(_ fields: HTTPFields, resources: HTTP3ResourceTracker) throws {
        let bytes = fields.reduce(0) {
            $0 + MemoryLayout<HTTPHeaderField>.stride + $1.name.rawName.utf8.count
                + $1.withUnsafeBytesOfValue { $0.count * (3 + MemoryLayout<UnicodeScalar>.stride) }
        }
        lease = bytes > 0 ? try resources.reserve(bytes, kind: .capture) : nil
        self.fields = fields.map { field in
            field.withUnsafeBytesOfValue {
                HTTPHeaderField(name: field.name.rawName, rawValueBytes: Array($0))
            }
        }
    }
}

enum HTTP3Fields {
    static func validate(_ fields: HTTPFields) throws {
        guard fields.count <= 1024 else { throw HTTP3ProxyError.resourceLimit }
        var remaining = 32_768
        for field in fields {
            try field.withUnsafeBytesOfValue { bytes in
                let count = field.name.rawName.utf8.count + bytes.count + 32
                guard count <= remaining else { throw HTTP3ProxyError.resourceLimit }
                guard HTTPField.isValidValue(bytes), permitted(name: field.name.canonicalName, value: bytes) else {
                    throw HTTP3ProxyError.protocolViolation
                }
                remaining -= count
            }
        }
    }

    static func encode(_ fields: [HTTPHeaderField]) throws -> HTTPFields {
        var result = HTTPFields()
        var remaining = 32_768
        guard fields.count <= 1024 else { throw HTTP3ProxyError.resourceLimit }
        result.reserveCapacity(fields.count)
        for field in fields {
            let nameText = field.name.lowercased()
            guard let name = HTTPField.Name(nameText) else { throw HTTPControlError.invalidHeaders }
            let bytes = field.rawValueBytes ?? Array(field.value.utf8)
            let count = nameText.utf8.count + bytes.count + 32
            guard count <= remaining else { throw HTTP3ProxyError.resourceLimit }
            remaining -= count
            guard HTTPField.isValidValue(bytes), permitted(name: nameText, value: bytes) else {
                throw HTTPControlError.invalidHeaders
            }
            result.append(HTTPField(name: name, value: bytes))
        }
        return result
    }

    static func encodedReservation(_ fields: [HTTPHeaderField]) -> Int {
        fields.reduce(0) {
            $0 + MemoryLayout<HTTPField>.stride + $1.name.utf8.count * 2 + $1.value.utf8.count * 2
        }
    }

    private static func permitted<Bytes: BidirectionalCollection>(name: String, value: Bytes) -> Bool
        where Bytes.Element == UInt8 {
        guard !["connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade"].contains(name) else {
            return false
        }
        guard name == "te" else { return true }
        let token = value.drop(while: { $0 == 32 || $0 == 9 })
            .reversed().drop(while: { $0 == 32 || $0 == 9 }).reversed()
        return token.count == 8 && zip(token, "trailers".utf8).allSatisfy { byte, expected in
            (byte >= 65 && byte <= 90 ? byte + 32 : byte) == expected
        }
    }

    static func requestAuthority(_ request: HTTPRequest) -> HTTPAuthority? {
        guard let authority = request.authority else { return nil }
        if let parsed = HTTPAuthority(authority: authority) { return parsed }
        let candidate = authority.hasSuffix("]") || !authority.contains(":") ? authority + ":443" : authority
        return HTTPAuthority(authority: candidate)
    }
}

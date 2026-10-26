import Foundation

struct HARJSONWriter {
    let writer: any HAROutputWriter
    private var buffer: [UInt8] = []
    private let sliceBytes = 65_536

    init(writer: any HAROutputWriter) { self.writer = writer }

    mutating func flush() async throws {
        guard !buffer.isEmpty else { return }
        try Task.checkCancellation()
        try await writer.write(buffer)
        buffer.removeAll(keepingCapacity: true)
        try Task.checkCancellation()
    }

    mutating func raw(_ value: String) async throws {
        for byte in value.utf8 { try await append(byte) }
    }

    private mutating func append(_ byte: UInt8) async throws {
        buffer.append(byte)
        if buffer.count == sliceBytes { try await flush() }
    }

    mutating func string(_ value: String) async throws {
        try await quotedUTF8(value.utf8)
    }

    mutating func quotedUTF8<Bytes: Sequence>(_ bytes: Bytes) async throws where Bytes.Element == UInt8 {
        try await append(34)
        for byte in bytes {
            switch byte {
            case 34: try await raw("\\\"")
            case 92: try await raw("\\\\")
            case 0...31:
                let hex = Array("0123456789abcdef".utf8)
                try await raw("\\u00")
                try await append(hex[Int(byte >> 4)])
                try await append(hex[Int(byte & 15)])
            default: try await append(byte)
            }
        }
        try await append(34)
    }

    mutating func fields(_ fields: [HTTPHeaderField], configuration: HARExportConfiguration) async throws {
        try await raw("[")
        for (index, field) in fields.enumerated() {
            if index > 0 { try await raw(",") }
            try await raw("{\"name\":")
            try await string(field.name)
            try await raw(",\"value\":")
            try await string(redactedHeader(field, configuration: configuration))
            try await raw("}")
        }
        try await raw("]")
    }

    private func redactedHeader(_ field: HTTPHeaderField, configuration: HARExportConfiguration) -> String {
        let name = field.name.lowercased()
        if configuration.redactedHeaderNames.contains(name) { return "[REDACTED]" }
        guard ["location", "referer", "content-location", "origin"].contains(name) else { return field.value }
        return redactedURL(field.value, configuration: configuration)?.string ?? "[REDACTED]"
    }

    func redactedURL(_ raw: String, configuration: HARExportConfiguration) -> URLComponents? {
        guard var components = URLComponents(string: raw) else { return nil }
        components.user = nil
        components.password = nil
        components.fragment = nil
        if let query = components.queryItems {
            components.queryItems = query.map {
                URLQueryItem(
                    name: $0.name,
                    value: configuration.redactedQueryNames.contains($0.name.lowercased()) ? "[REDACTED]" : $0.value
                )
            }
        }
        return components
    }

    func include(_ body: HARBody?, configuration: HARExportConfiguration) -> Bool {
        guard let body else { return false }
        guard case .includeUnredacted(let mimeTypes) = configuration.bodyPolicy else { return false }
        let mime = body.mimeType.split(separator: ";", maxSplits: 1).first.map(String.init)?.lowercased()
        return mime.map { mimeTypes.contains($0) } ?? false
    }

    func validText(_ body: HARBody?) throws -> Bool {
        guard let body, body.isText else { return false }
        var validator = HARUTF8Validator()
        for (index, byte) in body.bytes.enumerated() {
            if index & 65_535 == 0 { try Task.checkCancellation() }
            guard validator.consume(byte) else { return false }
        }
        return validator.complete
    }

    mutating func base64(_ bytes: [UInt8]) async throws {
        try await append(34)
        var offset = 0
        while offset < bytes.count {
            let end = min(offset + 49_152, bytes.count)
            try await raw(Data(bytes[offset..<end]).base64EncodedString())
            offset = end
        }
        try await append(34)
    }
}

private struct HARUTF8Validator {
    private var remaining = 0
    private var lower: UInt8 = 0x80
    private var upper: UInt8 = 0xBF

    var complete: Bool { remaining == 0 }

    mutating func consume(_ byte: UInt8) -> Bool {
        if remaining > 0 {
            guard byte >= lower, byte <= upper else { return false }
            remaining -= 1
            lower = 0x80
            upper = 0xBF
            return true
        }
        switch byte {
        case 0...0x7F: remaining = 0
        case 0xC2...0xDF: remaining = 1
        case 0xE0...0xEF: remaining = 2
        case 0xF0...0xF4: remaining = 3
        default: return false
        }
        lower = Self.lowerBound(byte)
        upper = Self.upperBound(byte)
        return true
    }

    private static func lowerBound(_ byte: UInt8) -> UInt8 {
        switch byte {
        case 0xE0: return 0xA0
        case 0xF0: return 0x90
        default: return 0x80
        }
    }

    private static func upperBound(_ byte: UInt8) -> UInt8 {
        switch byte {
        case 0xED: return 0x9F
        case 0xF4: return 0x8F
        default: return 0xBF
        }
    }
}

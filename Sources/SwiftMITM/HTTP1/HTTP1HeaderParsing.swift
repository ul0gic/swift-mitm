import Foundation

extension HTTP1MessageParser {
    enum ContentLengthResult {
        case none
        case value(Int)
        case invalid
    }

    static func parseHeaderFields(_ lines: [String]) -> [HTTPHeaderField] {
        lines.compactMap { line in
            guard let separator = line.firstIndex(of: ":") else { return nil }
            let name = line[line.startIndex..<separator].trimmingASCIIWhitespace()
            let value = line[line.index(after: separator)...].trimmingASCIIWhitespace()
            return name.isEmpty ? nil : HTTPHeaderField(name: name, value: value)
        }
    }

    static func isWebSocketUpgrade(_ headers: [HTTPHeaderField]) -> Bool {
        let upgrade = headers.first { $0.name.lowercased() == "upgrade" }?.value.lowercased()
        let connection = headers.first { $0.name.lowercased() == "connection" }?.value.lowercased()
        return upgrade?.contains("websocket") == true && connection?.contains("upgrade") == true
    }

    static func isChunked(_ headers: [HTTPHeaderField]) -> Bool {
        headers.contains {
            $0.name.lowercased() == "transfer-encoding" && $0.value.lowercased().contains("chunked")
        }
    }

    static func contentLength(_ headers: [HTTPHeaderField]) -> ContentLengthResult {
        let fields = headers.filter { $0.name.lowercased() == "content-length" }
        guard !fields.isEmpty else { return .none }
        var distinct: Set<Int> = []
        for field in fields {
            let text = field.value.trimmingASCIIWhitespace()
            guard !text.isEmpty, text.utf8.allSatisfy({ $0 >= 0x30 && $0 <= 0x39 }),
                let length = Int(text) else {
                return .invalid
            }
            distinct.insert(length)
        }
        guard distinct.count == 1, let length = distinct.first else { return .invalid }
        return .value(length)
    }

    static func parseChunkSize(_ line: [UInt8]) -> Int? {
        let withoutCRLF = line.dropLast(2)
        let hex = withoutCRLF.prefix { $0 != UInt8(ascii: ";") }
        let text = (String(bytes: hex, encoding: .utf8) ?? "").trimmingASCIIWhitespace()
        guard !text.isEmpty, text.utf8.allSatisfy(Self.isHexDigit) else { return nil }
        return Int(text, radix: 16)
    }

    static func isHexDigit(_ byte: UInt8) -> Bool {
        (byte >= 0x30 && byte <= 0x39) || (byte >= 0x41 && byte <= 0x46) || (byte >= 0x61 && byte <= 0x66)
    }

    static func endsWithDoubleCRLF(_ bytes: [UInt8]) -> Bool {
        bytes.count >= 4
            && bytes[bytes.count - 4] == 13 && bytes[bytes.count - 3] == 10
            && bytes[bytes.count - 2] == 13 && bytes[bytes.count - 1] == 10
    }

    static func endsWithCRLF(_ bytes: [UInt8]) -> Bool {
        bytes.count >= 2 && bytes[bytes.count - 2] == 13 && bytes[bytes.count - 1] == 10
    }
}

private extension StringProtocol {
    func trimmingASCIIWhitespace() -> String {
        String(drop { $0 == " " || $0 == "\t" }.reversed().drop { $0 == " " || $0 == "\t" }.reversed())
    }
}

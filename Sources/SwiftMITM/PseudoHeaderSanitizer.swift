import NIOHPACK

public enum HeaderSanitizationError: Error, Equatable, Sendable {
    case missingPseudoHeader(String)
    case duplicatePseudoHeader(String)
    case illegalCharacterInPseudoHeader(String)
    case illegalCharacterInHeader(String)
    case uppercaseHeaderName(String)
    case invalidMethod
    case invalidScheme
    case invalidPath
    case invalidAuthority
    case emptyHeaderName
    case invalidHeaderName(String)
    case connectionSpecificHeader(String)
    case conflictingContentLength
    case illegalContentLength
    case illegalTransferEncoding
}

public struct SanitizedRequestLine: Sendable, Equatable {
    public let method: String
    public let scheme: String
    public let authority: String
    public let path: String
    public let headers: [HTTPHeaderField]
}

public enum PseudoHeaderSanitizer {
    private static let connectionSpecific: Set<String> = [
        "connection", "keep-alive", "proxy-connection", "upgrade"
    ]

    public static func sanitizeRequest(
        _ headers: HPACKHeaders
    ) -> Result<SanitizedRequestLine, HeaderSanitizationError> {
        do {
            let method = try requirePseudoHeader(":method", in: headers)
            let scheme = try requirePseudoHeader(":scheme", in: headers)
            let authority = try requirePseudoHeader(":authority", in: headers)
            let path = try requirePseudoHeader(":path", in: headers)

            guard isToken(method) else { throw HeaderSanitizationError.invalidMethod }
            guard isScheme(scheme) else { throw HeaderSanitizationError.invalidScheme }
            guard path.first == "/" || (method == "OPTIONS" && path == "*") else {
                throw HeaderSanitizationError.invalidPath
            }
            guard !containsWhitespace(path) else { throw HeaderSanitizationError.invalidPath }
            guard !authority.contains("/"), !containsWhitespace(authority) else {
                throw HeaderSanitizationError.invalidAuthority
            }

            let regular = try sanitizeRegularHeaders(headers)
            return .success(
                SanitizedRequestLine(
                    method: method,
                    scheme: scheme,
                    authority: authority,
                    path: path,
                    headers: regular
                )
            )
        } catch let error as HeaderSanitizationError {
            return .failure(error)
        } catch {
            return .failure(.illegalCharacterInHeader("unknown"))
        }
    }

    private static func requirePseudoHeader(
        _ name: String,
        in headers: HPACKHeaders
    ) throws -> String {
        let values = headers[name]
        guard let value = values.first else {
            throw HeaderSanitizationError.missingPseudoHeader(name)
        }
        guard values.count == 1 else {
            throw HeaderSanitizationError.duplicatePseudoHeader(name)
        }
        guard !containsControlCharacter(value) else {
            throw HeaderSanitizationError.illegalCharacterInPseudoHeader(name)
        }
        return value
    }

    private static func sanitizeRegularHeaders(
        _ headers: HPACKHeaders
    ) throws -> [HTTPHeaderField] {
        var result: [HTTPHeaderField] = []
        var contentLengths: Set<String> = []
        var sawTransferEncoding = false

        for (name, value, _) in headers where !name.hasPrefix(":") {
            let lower = name.lowercased()
            try validateRegularHeader(name: name, lower: lower, value: value)
            if lower == "transfer-encoding" {
                sawTransferEncoding = true
                continue
            }
            if lower == "content-length" {
                try recordContentLength(value, into: &contentLengths)
            }
            result.append(HTTPHeaderField(name: lower, value: value))
        }

        if sawTransferEncoding {
            throw HeaderSanitizationError.illegalTransferEncoding
        }
        if contentLengths.count > 1 {
            throw HeaderSanitizationError.conflictingContentLength
        }
        return result
    }

    private static func validateRegularHeader(name: String, lower: String, value: String) throws {
        if name.isEmpty {
            throw HeaderSanitizationError.emptyHeaderName
        }
        if name != lower {
            throw HeaderSanitizationError.uppercaseHeaderName(name)
        }
        guard isToken(name) else {
            throw HeaderSanitizationError.invalidHeaderName(name)
        }
        if containsControlCharacter(value) {
            throw HeaderSanitizationError.illegalCharacterInHeader(name)
        }
        if connectionSpecific.contains(lower) {
            throw HeaderSanitizationError.connectionSpecificHeader(lower)
        }
        if lower == "te", value.lowercased() != "trailers" {
            throw HeaderSanitizationError.connectionSpecificHeader("te")
        }
    }

    private static func recordContentLength(_ value: String, into set: inout Set<String>) throws {
        guard !value.isEmpty, value.utf8.allSatisfy({ $0 >= 0x30 && $0 <= 0x39 }) else {
            throw HeaderSanitizationError.illegalContentLength
        }
        set.insert(value)
    }

    private static func containsControlCharacter(_ value: String) -> Bool {
        value.utf8.contains { $0 < 0x20 || $0 == 0x7F }
    }

    private static func containsWhitespace(_ value: String) -> Bool {
        value.utf8.contains { $0 == 0x20 || $0 == 0x09 }
    }

    private static func isToken(_ value: String) -> Bool {
        !value.isEmpty && value.utf8.allSatisfy(isTokenByte)
    }

    private static func isScheme(_ value: String) -> Bool {
        guard let first = value.utf8.first, isASCIIAlpha(first) else { return false }
        return value.utf8.dropFirst().allSatisfy { byte in
            isASCIIAlpha(byte) || (byte >= 0x30 && byte <= 0x39)
                || byte == 0x2B || byte == 0x2D || byte == 0x2E
        }
    }

    private static func isASCIIAlpha(_ byte: UInt8) -> Bool {
        (byte >= 0x41 && byte <= 0x5A) || (byte >= 0x61 && byte <= 0x7A)
    }

    /// RFC 7230 token: any VCHAR except delimiters. Excludes control chars, whitespace, and separators.
    private static func isTokenByte(_ byte: UInt8) -> Bool {
        switch byte {
        case 0x30...0x39, 0x41...0x5A, 0x61...0x7A:
            return true
        case 0x21, 0x23, 0x24, 0x25, 0x26, 0x27, 0x2A, 0x2B, 0x2D, 0x2E,
             0x5E, 0x5F, 0x60, 0x7C, 0x7E:
            return true
        default:
            return false
        }
    }
}

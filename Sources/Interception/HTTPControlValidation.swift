import Foundation

extension HTTPControlConfiguration {
    static func ruleBytes(_ rule: HTTPRule, limits: HTTPControlLimits) throws -> Int {
        if let authority = rule.authority {
            guard let target = ConnectionTarget(
                explicitConnectAuthority: authority, ingressProvenance: .explicitHTTP
            ), target.logicalAuthority == authority else { throw HTTPControlError.invalidConfiguration }
        }
        let matching = rule.id.utf8.count + (rule.authority?.utf8.count ?? 0)
            + (rule.method?.utf8.count ?? 0) + (rule.path?.utf8.count ?? 0)
        return matching + (try headerBytes(rule.matchingHeaders, multiplexed: false))
            + (try actionBytes(rule.action, limits: limits))
    }

    static func actionBytes(_ action: HTTPControlAction, limits: HTTPControlLimits) throws -> Int {
        switch action {
        case .editHeaders(let remove, let append):
            let reserved: Set = [
                "host", "content-length", "transfer-encoding", "connection", "upgrade", "trailer", "content-encoding"
            ]
            guard !remove.contains(where: { reserved.contains($0.lowercased()) }),
                  !append.contains(where: { reserved.contains($0.name.lowercased()) }) else {
                throw HTTPControlError.unsupportedEdit
            }
            return remove.reduce(0) { $0 + $1.utf8.count } + (try headerBytes(append, multiplexed: false))
        case .replaceBody(let body, let maximum, let encoding, _):
            try validateReplacement(body, maximum: maximum, encoding: encoding, limits: limits)
            return body.count + (encoding?.utf8.count ?? 0)
        case .respond(let status, let headers, let body):
            try validateResponse(status: status, body: body, limits: limits)
            return body.count + (try headerBytes(headers, multiplexed: false))
        case .route(let authority, let sensitive):
            guard ConnectionTarget(explicitConnectAuthority: authority, ingressProvenance: .explicitHTTP) != nil else {
                throw HTTPControlError.invalidConfiguration
            }
            return authority.utf8.count + sensitive.reduce(0) { $0 + $1.utf8.count }
        case .fault(let fault):
            try fault.validate()
            return 0
        case .forward, .breakpoint, .abort:
            return 0
        }
    }

    static func validateReplacement(
        _ body: Data, maximum: Int, encoding: String?, limits: HTTPControlLimits
    ) throws {
        guard (1...limits.maximumBodyBytes).contains(maximum), body.count <= limits.maximumBodyBytes,
              encoding?.utf8.allSatisfy({ $0 >= 32 && $0 < 127 }) ?? true else {
            throw HTTPControlError.invalidConfiguration
        }
    }

    static func validateResponse(status: Int, body: Data, limits: HTTPControlLimits) throws {
        guard (200...599).contains(status), body.count <= limits.maximumBodyBytes,
              ![204, 205, 304].contains(status) || body.isEmpty else { throw HTTPControlError.unsupportedEdit }
    }

    static func terminalHeaders(
        _ action: HTTPControlAction, headers: [HTTPHeaderField], message: HTTPControlMessage
    ) throws -> [HTTPHeaderField] {
        switch action {
        case .replaceBody(let body, let maximum, let encoding, let override):
            try requireBodyEdit(message, overrideNoTransform: override)
            guard let input = message.body, input.count <= maximum else { throw HTTPControlError.bodyLimitExceeded }
            return replacingBodyHeaders(headers, length: body.count, encoding: encoding)
        case .route(let authority, _):
            guard message.phase == .request, !unsupported(message) else { throw HTTPControlError.unsupportedEdit }
            let removed = authority == message.authority ? Set<String>() : sensitiveNames(action)
            var result = headers.filter { !removed.contains($0.name.lowercased()) && $0.name.lowercased() != "host" }
            result.append(HTTPHeaderField(name: "host", value: authority))
            return result
        case .respond(let status, let fields, let body):
            guard message.status != 101, message.method != "HEAD" || body.isEmpty else { throw HTTPControlError.unsupportedEdit }
            var result = fields.filter { !["content-length", "transfer-encoding"].contains($0.name.lowercased()) }
            if ![204, 304].contains(status) {
                result.append(HTTPHeaderField(name: "content-length", value: String(body.count)))
            }
            return result
        case .breakpoint:
            guard !isUpgrade(message) else { throw HTTPControlError.unsupportedEdit }
            return headers
        case .forward, .editHeaders, .fault, .abort:
            return headers
        }
    }

    static func requireBodyEdit(_ message: HTTPControlMessage, overrideNoTransform: Bool) throws {
        guard !unsupported(message), !message.headers.contains(where: { $0.name.lowercased() == "expect" }) else {
            throw HTTPControlError.unsupportedEdit
        }
        guard overrideNoTransform || !hasNoTransform(message.headers) else { throw HTTPControlError.noTransform }
    }

    static func hasNoTransform(_ headers: [HTTPHeaderField]) -> Bool {
        headers.contains { field in
            field.name.lowercased() == "cache-control" && field.value.lowercased().split(separator: ",").contains {
                $0.trimmingCharacters(in: .whitespaces) == "no-transform"
            }
        }
    }

    static func replacingBodyHeaders(
        _ headers: [HTTPHeaderField], length: Int, encoding: String?
    ) -> [HTTPHeaderField] {
        let stale: Set = [
            "content-length", "transfer-encoding", "etag", "last-modified", "content-md5", "digest", "content-digest",
            "repr-digest", "content-encoding", "content-range", "accept-ranges", "trailer"
        ]
        var result = headers.filter { !stale.contains($0.name.lowercased()) }
        result.append(HTTPHeaderField(name: "content-length", value: String(length)))
        if let encoding {
            result.append(HTTPHeaderField(name: "content-encoding", value: encoding))
        }
        return result
    }
}

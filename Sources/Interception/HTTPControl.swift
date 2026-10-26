import Foundation

public enum HTTPControlError: Error, Sendable {
    case invalidConfiguration, invalidHeaders, unsupportedEdit, noTransform, bodyLimitExceeded
    case capacityExceeded, cancelled, deadlineExceeded
}

public enum HTTPControlPhase: Sendable { case request, response }

public struct HTTPControlContext: Sendable {
    public let requestID: UUID
    public let authority: String
    public let method: String
    public let path: String
    public let isHTTP2OrLater: Bool

    public init(
        requestID: UUID,
        authority: String,
        method: String,
        path: String,
        isHTTP2OrLater: Bool = false
    ) {
        self.requestID = requestID
        self.authority = authority
        self.method = method
        self.path = path
        self.isHTTP2OrLater = isHTTP2OrLater
    }
}

public struct HTTPControlMessage: Sendable {
    public let context: HTTPControlContext
    public let phase: HTTPControlPhase
    public let status: Int?
    public let headers: [HTTPHeaderField]
    public let body: Data?
    public var requestID: UUID { context.requestID }
    public var authority: String { context.authority }
    public var method: String { context.method }
    public var path: String { context.path }
    public var isHTTP2OrLater: Bool { context.isHTTP2OrLater }

    public init(
        context: HTTPControlContext,
        phase: HTTPControlPhase,
        status: Int? = nil,
        headers: [HTTPHeaderField],
        body: Data? = nil
    ) {
        self.context = context
        self.phase = phase
        self.status = status
        self.headers = headers
        self.body = body
    }

    var retainedBytes: Int {
        authority.utf8.count + method.utf8.count + path.utf8.count
            + headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
            + (body?.count ?? 0)
    }
}

public struct HTTPControlLimits: Sendable {
    public let maximumRules: Int
    public let maximumConfigurationBytes: Int
    public let maximumTasks: Int
    public let maximumBodyBytes: Int
    public let maximumRetainedBytes: Int

    public init(
        maximumRules: Int = 256,
        maximumConfigurationBytes: Int = 262_144,
        maximumTasks: Int = 16,
        maximumBodyBytes: Int = 1_048_576,
        maximumRetainedBytes: Int = 16_777_216
    ) throws {
        guard (1...1024).contains(maximumRules), (1...1_048_576).contains(maximumConfigurationBytes),
              (1...128).contains(maximumTasks), (1...16_777_216).contains(maximumBodyBytes),
              (1...67_108_864).contains(maximumRetainedBytes),
              maximumBodyBytes <= maximumRetainedBytes else { throw HTTPControlError.invalidConfiguration }
        self.maximumRules = maximumRules
        self.maximumConfigurationBytes = maximumConfigurationBytes
        self.maximumTasks = maximumTasks
        self.maximumBodyBytes = maximumBodyBytes
        self.maximumRetainedBytes = maximumRetainedBytes
    }
}

public struct HTTPRuleMatch: Sendable {
    public let phase: HTTPControlPhase
    public let authority: String?
    public let method: String?
    public let path: String?
    public let headers: [HTTPHeaderField]

    public init(
        phase: HTTPControlPhase,
        authority: String? = nil,
        method: String? = nil,
        path: String? = nil,
        headers: [HTTPHeaderField] = []
    ) {
        self.phase = phase
        self.authority = authority
        self.method = method
        self.path = path
        self.headers = headers
    }
}

public struct HTTPRule: Sendable {
    public let id: String
    public let match: HTTPRuleMatch
    public let action: HTTPControlAction
    public var phase: HTTPControlPhase { match.phase }
    public var authority: String? { match.authority }
    public var method: String? { match.method }
    public var path: String? { match.path }
    public var matchingHeaders: [HTTPHeaderField] { match.headers }

    public init(id: String, match: HTTPRuleMatch, action: HTTPControlAction) {
        self.id = id
        self.match = match
        self.action = action
    }

    func matches(_ message: HTTPControlMessage) -> Bool {
        phase == message.phase && (authority == nil || authority == message.authority)
            && (method == nil || method == message.method) && (path == nil || path == message.path)
            && matchingHeaders.allSatisfy { predicate in
                message.headers.contains {
                    $0.name.lowercased() == predicate.name.lowercased()
                        && $0.value.utf8.elementsEqual(predicate.value.utf8)
                }
            }
    }
}

public enum HTTPControlAction: Sendable {
    case forward
    case editHeaders(remove: [String], append: [HTTPHeaderField])
    case replaceBody(Data, maximumInputBytes: Int, contentEncoding: String?, overrideNoTransform: Bool)
    case respond(status: Int, headers: [HTTPHeaderField], body: Data)
    case route(authority: String, sensitiveHeaders: [String])
    case breakpoint
    case fault(HTTPFault)
    case abort
}

public struct HTTPControlDecision: Sendable {
    public let headers: [HTTPHeaderField]
    public let action: HTTPControlAction
    public let appliedRuleIDs: [String]
    public let faults: [HTTPFault]
    public let overridesNoTransform: Bool
    public let syntheticResponseHeaders: [HTTPHeaderField]?

    var retainedBytes: Int {
        var bytes = headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
        bytes += appliedRuleIDs.reduce(0) { $0 + $1.utf8.count }
        bytes += syntheticResponseHeaders?.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count } ?? 0
        switch action {
        case .replaceBody(let body, _, let encoding, _):
            bytes += body.count + (encoding?.utf8.count ?? 0)
        case .respond(_, let headers, let body):
            bytes += body.count + headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
        case .route(let authority, let sensitive):
            bytes += authority.utf8.count + sensitive.reduce(0) { $0 + $1.utf8.count }
        case .editHeaders(let remove, let append):
            bytes += remove.reduce(0) { $0 + $1.utf8.count }
                + append.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
        default: break
        }
        return bytes
    }
}

public enum HTTPControlHeadResult: Sendable {
    case decision(HTTPControlDecision)
    case needsBody(maximumBytes: Int)
}

public struct HTTPControlConfiguration: Sendable {
    public let rules: [HTTPRule]
    public let limits: HTTPControlLimits
    public let breakpointDeadlineSeconds: Int
    public let breakpointExpiry: HTTPBreakpointExpiry
    let retainedBytes: Int

    public init(
        rules: [HTTPRule],
        limits: HTTPControlLimits,
        breakpointDeadlineSeconds: Int = 30,
        breakpointExpiry: HTTPBreakpointExpiry = .reject
    ) throws {
        guard (1...300).contains(breakpointDeadlineSeconds) else { throw HTTPControlError.invalidConfiguration }
        guard rules.count <= limits.maximumRules else { throw HTTPControlError.capacityExceeded }
        var bytes = 0
        var identifiers = Set<String>()
        for rule in rules {
            guard !rule.id.isEmpty, identifiers.insert(rule.id).inserted else { throw HTTPControlError.invalidConfiguration }
            bytes += try Self.ruleBytes(rule, limits: limits)
            guard bytes <= limits.maximumConfigurationBytes else { throw HTTPControlError.capacityExceeded }
        }
        self.rules = rules
        self.limits = limits
        self.breakpointDeadlineSeconds = breakpointDeadlineSeconds
        self.breakpointExpiry = breakpointExpiry
        retainedBytes = bytes
    }

    public func evaluate(_ message: HTTPControlMessage) throws -> HTTPControlDecision {
        var headers = message.headers
        var applied: [String] = []
        var faults: [HTTPFault] = []
        var outcome = HTTPControlAction.forward
        var override = false
        var synthetic: [HTTPHeaderField]?
        rulesLoop: for rule in rules where rule.matches(message) {
            applied.append(rule.id)
            switch rule.action {
            case .forward:
                continue
            case .editHeaders(let remove, let append):
                guard !Self.unsupported(message) else { throw HTTPControlError.unsupportedEdit }
                let names = Set(remove.map { $0.lowercased() })
                headers.removeAll { names.contains($0.name.lowercased()) }
                headers.append(contentsOf: append)
            case .fault(let fault):
                guard !Self.isUpgrade(message) else { throw HTTPControlError.unsupportedEdit }
                faults.append(fault)
            default:
                let terminal = try Self.terminalHeaders(rule.action, headers: headers, message: message)
                if message.phase == .request, case .respond = rule.action { synthetic = terminal }
                else { headers = terminal }
                outcome = rule.action
                if case .replaceBody(_, _, _, let explicit) = outcome { override = explicit }
                break rulesLoop
            }
        }
        if !applied.isEmpty { _ = try Self.headerBytes(headers, multiplexed: message.isHTTP2OrLater) }
        if let synthetic { _ = try Self.headerBytes(synthetic, multiplexed: message.isHTTP2OrLater) }
        return HTTPControlDecision(
            headers: headers, action: outcome, appliedRuleIDs: applied,
            faults: faults, overridesNoTransform: override, syntheticResponseHeaders: synthetic
        )
    }

    public func evaluateHead(_ message: HTTPControlMessage) throws -> HTTPControlHeadResult {
        for rule in rules where rule.matches(message) {
            switch rule.action {
            case .replaceBody(_, let maximum, _, let override):
                try Self.requireBodyEdit(message, overrideNoTransform: override)
                return .needsBody(maximumBytes: maximum)
            case .forward, .editHeaders, .fault: continue
            default: return .decision(try evaluate(message))
            }
        }
        return .decision(try evaluate(message))
    }

    static func unsupported(_ message: HTTPControlMessage) -> Bool {
        message.method == "CONNECT" || message.method == "HEAD"
            || (message.status.map { $0 < 200 || [204, 205, 304].contains($0) } ?? false)
            || message.headers.contains { $0.name.lowercased() == "upgrade" }
    }

    static func isUpgrade(_ message: HTTPControlMessage) -> Bool {
        message.method == "CONNECT" || message.status == 101
            || message.headers.contains { $0.name.lowercased() == "upgrade" }
    }

    static func sensitiveNames(_ action: HTTPControlAction) -> Set<String> {
        if case .route(_, let extra) = action {
            return Set(["authorization", "proxy-authorization", "cookie"] + extra.map { $0.lowercased() })
        }
        return []
    }

    static func headerBytes(_ headers: [HTTPHeaderField], multiplexed: Bool) throws -> Int {
        var bytes = 0
        var hasLength = false
        var transfer = false
        for header in headers {
            try validateField(header, multiplexed: multiplexed)
            let name = header.name.lowercased()
            if name == "content-length" { hasLength = true }
            if name == "transfer-encoding" { transfer = true }
            bytes += header.name.utf8.count + header.value.utf8.count
        }
        guard !hasLength || !transfer else { throw HTTPControlError.invalidHeaders }
        if case .invalid = HTTP1MessageParser.contentLength(headers) { throw HTTPControlError.invalidHeaders }
        return bytes
    }

    static func validateField(_ field: HTTPHeaderField, multiplexed: Bool) throws {
        guard !field.name.isEmpty, field.name.utf8.allSatisfy({
            (48...57).contains($0) || (65...90).contains($0) || (97...122).contains($0)
                || "!#$%&'*+-.^_`|~".utf8.contains($0)
        }), multiplexed || field.value.unicodeScalars.allSatisfy({ $0.value <= 255 }),
        field.value.utf8.allSatisfy({ $0 == 9 || ($0 >= 32 && $0 != 127) }) else {
            throw HTTPControlError.invalidHeaders
        }
        let connectionFields = ["connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade"]
        guard !multiplexed || !connectionFields.contains(field.name.lowercased()) else {
            throw HTTPControlError.invalidHeaders
        }
        if multiplexed, field.name.lowercased() == "te",
           field.value.lowercased().trimmingCharacters(in: .whitespaces) != "trailers" {
            throw HTTPControlError.invalidHeaders
        }
    }
}

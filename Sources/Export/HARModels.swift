public enum HARExportError: Error, Sendable {
    case invalidURL
    case mismatchedRepresentation
    case incompleteExchange
    case missingTimings
    case invalidTimings
    case missingContentSize
    case invalidContentSize
}

public struct HARExchangeTiming: Sendable {
    public let send: Double
    public let wait: Double
    public let receive: Double
    public let blocked: Double?
    public let dns: Double?
    public let connect: Double?
    public let ssl: Double?
    public let total: Double

    public init(
        send: Double,
        wait: Double,
        receive: Double,
        blocked: Double? = nil,
        dns: Double? = nil,
        connect: Double? = nil,
        ssl: Double? = nil
    ) throws {
        let phases = [send, wait, receive] + [blocked, dns, connect, ssl].compactMap(\.self)
        let total = send + wait + receive + (blocked ?? 0) + (dns ?? 0) + (connect ?? 0)
        guard phases.allSatisfy({ $0.isFinite && $0 >= 0 }), total.isFinite,
            ssl.map({ value in connect.map { value <= $0 } ?? false }) ?? true
        else { throw HARExportError.invalidTimings }
        self.send = send
        self.wait = wait
        self.receive = receive
        self.blocked = blocked
        self.dns = dns
        self.connect = connect
        self.ssl = ssl
        self.total = total
    }
}

public enum HARBodyPolicy: Sendable {
    case omit
    case includeUnredacted(mimeTypes: Set<String>)
}

public struct HARExportConfiguration: Sendable {
    public let representation: HTTPRepresentationKind
    public let redactedHeaderNames: Set<String>
    public let redactedQueryNames: Set<String>
    public let bodyPolicy: HARBodyPolicy
    public let includeBinaryRequestExtension: Bool

    public init(
        representation: HTTPRepresentationKind,
        redactedHeaderNames: Set<String> = [],
        redactedQueryNames: Set<String> = ["access_token", "token", "api_key", "password"],
        bodyPolicy: HARBodyPolicy = .omit,
        includeBinaryRequestExtension: Bool = false
    ) {
        self.representation = representation
        self.redactedHeaderNames = Set(redactedHeaderNames.map { $0.lowercased() })
            .union(["authorization", "proxy-authorization", "cookie", "set-cookie"])
        self.redactedQueryNames = Set(redactedQueryNames.map { $0.lowercased() })
        self.bodyPolicy = bodyPolicy
        self.includeBinaryRequestExtension = includeBinaryRequestExtension
    }
}

public struct HARBody: Sendable {
    public let bytes: [UInt8]
    public let mimeType: String
    public let isText: Bool
    public let truncated: Bool
    public let observedByteCount: Int?

    public init(
        bytes: [UInt8],
        mimeType: String,
        isText: Bool,
        truncated: Bool,
        observedByteCount: Int? = nil
    ) {
        self.bytes = bytes
        self.mimeType = mimeType
        self.isText = isText
        self.truncated = truncated
        self.observedByteCount = observedByteCount
    }
}

public struct HARExchange: Sendable {
    public let representation: HTTPRepresentationKind
    public let request: CapturedRequestHead
    public let response: CapturedResponseHead
    public let requestBody: HARBody?
    public let responseBody: HARBody?
    public let completed: Bool
    public let timings: HARExchangeTiming?
    public let responseContentBytes: Int?

    public init(
        representation: HTTPRepresentationKind,
        request: CapturedRequestHead,
        response: CapturedResponseHead,
        requestBody: HARBody? = nil,
        responseBody: HARBody? = nil,
        completed: Bool,
        timings: HARExchangeTiming? = nil,
        responseContentBytes: Int? = nil
    ) {
        self.representation = representation
        self.request = request
        self.response = response
        self.requestBody = requestBody
        self.responseBody = responseBody
        self.completed = completed
        self.timings = timings
        self.responseContentBytes = responseContentBytes
    }
}

public protocol HAROutputWriter: Sendable {
    func write(_ bytes: [UInt8]) async throws
}

import NIOHTTP1

extension ExplicitHTTPProxyHandler {
    static func fields(_ headers: HTTPHeaders) -> [HTTPHeaderField] {
        headers.map { HTTPHeaderField(name: $0.name, value: $0.value) }
    }

    static func isWebSocket(_ headers: HTTPHeaders) -> Bool {
        HTTP1MessageParser.isWebSocketUpgrade(fields(headers))
    }

    static func ambiguous(_ headers: HTTPHeaders) -> Bool {
        let fields = fields(headers)
        if headers.contains(name: "transfer-encoding"), headers.contains(name: "content-length") { return true }
        if case .invalid = HTTP1MessageParser.contentLength(fields) { return true }
        let codings = headers[canonicalForm: "transfer-encoding"].map { $0.lowercased() }
        if codings.dropLast().contains("chunked") { return true }
        return false
    }

    static func hopHeaders(
        _ original: HTTPHeaders,
        upgrading: Bool,
        appendVia: Bool = true,
        excluding excluded: Set<String> = []
    ) -> HTTPHeaders {
        let nominated = HTTP1MessageParser.tokens(fields(original), named: "connection")
        let removed: Set<String> = [
            "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
            "te", "trailer", "transfer-encoding", "upgrade"
        ]
        var headers = HTTPHeaders(original.filter {
            !removed.contains($0.name.lowercased()) && !nominated.contains($0.name.lowercased())
                && !excluded.contains($0.name.lowercased())
        }.map { ($0.name, $0.value) })
        if appendVia { headers.add(name: "via", value: "1.1 SwiftMITM") }
        if upgrading {
            headers.replaceOrAdd(name: "connection", value: "upgrade")
            headers.replaceOrAdd(name: "upgrade", value: "websocket")
        }
        return headers
    }

    static func frameRequest(_ head: inout HTTPRequestHead, original: HTTPHeaders) {
        if original.contains(name: "transfer-encoding") {
            head.headers.replaceOrAdd(
                name: "transfer-encoding",
                value: original["transfer-encoding"].joined(separator: ", ")
            )
        } else if original.contains(name: "content-length"), !head.headers.contains(name: "content-length") {
            head.headers.replaceOrAdd(name: "transfer-encoding", value: "chunked")
        }
    }

    static func frameResponse(_ headers: inout HTTPHeaders, original: HTTPHeaders) {
        if original.contains(name: "transfer-encoding") {
            let value = original["transfer-encoding"].joined(separator: ", ")
            let chunked = HTTP1MessageParser.tokens(fields(original), named: "transfer-encoding").contains("chunked")
            headers.replaceOrAdd(name: "transfer-encoding", value: chunked ? value : value + ", chunked")
        } else if !headers.contains(name: "content-length") {
            headers.replaceOrAdd(name: "transfer-encoding", value: "chunked")
        }
    }
}

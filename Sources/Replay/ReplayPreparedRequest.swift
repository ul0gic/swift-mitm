import Foundation
import NIOHTTP1

struct ReplayPreparedRequest: Sendable {
    let target: HTTPForwardTarget
    let head: HTTPRequestHead

    init(_ request: ReplayRequest) throws {
        let target = try Self.target(request)
        let preserve = try Self.preserveCredentials(request, target: target)
        var headers = try Self.headers(request, preserveCredentials: preserve)
        headers = ExplicitHTTPProxyHandler.hopHeaders(headers, upgrading: false)
        headers.replaceOrAdd(name: "host", value: target.authority)
        headers.replaceOrAdd(name: "content-length", value: String(request.body.count))
        headers.replaceOrAdd(name: "connection", value: "close")
        self.target = target
        head = HTTPRequestHead(
            version: .http1_1, method: HTTPMethod(rawValue: request.method),
            uri: target.requestTarget, headers: headers
        )
    }

    func applying(_ decision: HTTPControlDecision?, to request: ReplayRequest) throws -> ReplayRequest {
        guard let decision else { return request }
        var destination = request.destination
        var body = request.body
        var policy = request.credentialPolicy
        var sensitive = request.sensitiveHeaderNames
        switch decision.action {
        case .abort: throw HTTPControlError.cancelled
        case .breakpoint: throw HTTPControlError.unsupportedEdit
        case .replaceBody(let replacement, _, _, _): body = Array(replacement)
        case .route(let authority, let names):
            guard let mapped = HTTPForwardTarget(
                scheme: target.scheme, authority: authority, requestTarget: target.requestTarget
            ), let url = URL(string: mapped.scheme + "://" + mapped.authority + mapped.requestTarget)
            else { throw ReplayError.invalidRequest }
            destination = url
            if mapped.authority != target.authority { policy = .strip }
            sensitive.formUnion(names)
        case .forward, .editHeaders, .respond, .fault: break
        }
        return ReplayRequest(
            source: request.source, destination: destination, method: request.method,
            headers: decision.headers, body: body, bodyIsComplete: true,
            credentialPolicy: policy, sensitiveHeaderNames: sensitive, useControls: false
        )
    }

    private static func target(_ request: ReplayRequest) throws -> HTTPForwardTarget {
        guard let components = URLComponents(url: request.destination, resolvingAgainstBaseURL: false),
            let scheme = components.scheme?.lowercased(), scheme == "http" || scheme == "https",
            let host = components.host, !host.isEmpty, components.user == nil, components.password == nil,
            components.fragment == nil, !request.method.isEmpty, request.method.utf8.allSatisfy(token)
        else { throw ReplayError.invalidRequest }
        guard request.method.uppercased() != "CONNECT", !request.headers.contains(where: {
            $0.name.lowercased() == "upgrade" || $0.name.lowercased().hasPrefix("sec-websocket-")
        }) else { throw ReplayError.unsupportedSession }
        let port = components.port ?? (scheme == "https" ? 443 : 80)
        let authorityHost = host.contains(":") && !host.hasPrefix("[") ? "[" + host + "]" : host
        let authority = authorityHost + ":" + String(port)
        var path = components.percentEncodedPath.isEmpty ? "/" : components.percentEncodedPath
        if let query = components.percentEncodedQuery { path += "?" + query }
        guard let target = HTTPForwardTarget(scheme: scheme, authority: authority, requestTarget: path) else {
            throw ReplayError.invalidRequest
        }
        return target
    }

    private static func preserveCredentials(_ request: ReplayRequest, target: HTTPForwardTarget) throws -> Bool {
        guard case .preserveForSameOrigin(let source) = request.credentialPolicy else { return false }
        guard let source = URLComponents(url: source, resolvingAgainstBaseURL: false),
            source.scheme?.lowercased() == target.scheme, let host = source.host
        else { throw ReplayError.credentialScopeMismatch }
        let unbracketed = host.hasPrefix("[") && host.hasSuffix("]") ? String(host.dropFirst().dropLast()) : host
        guard ConnectionTarget.normalizedHost(unbracketed) == target.host,
            (source.port ?? (target.scheme == "https" ? 443 : 80)) == target.port
        else { throw ReplayError.credentialScopeMismatch }
        return true
    }

    private static func headers(_ request: ReplayRequest, preserveCredentials: Bool) throws -> HTTPHeaders {
        let sensitive = Set(request.sensitiveHeaderNames.map { $0.lowercased() })
            .union(["authorization", "cookie"])
        var headers = HTTPHeaders()
        for field in request.headers {
            guard !field.name.isEmpty, field.name.utf8.allSatisfy(token),
                field.value.unicodeScalars.allSatisfy({
                    $0.value <= 255 && ($0.value == 9 || ($0.value >= 32 && $0.value != 127))
                })
            else { throw ReplayError.invalidRequest }
            let name = field.name.lowercased()
            if name == "proxy-authorization" || (!preserveCredentials && sensitive.contains(name)) { continue }
            headers.add(name: field.name, value: field.value)
        }
        return headers
    }

    private static func token(_ byte: UInt8) -> Bool {
        (48...57).contains(byte) || (65...90).contains(byte) || (97...122).contains(byte)
            || [UInt8(33), 35, 36, 37, 38, 39, 42, 43, 45, 46, 94, 95, 96, 124, 126].contains(byte)
    }
}

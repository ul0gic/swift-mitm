import Foundation
import SwiftMITM

enum HostControlPreset {
    static func configuration(_ name: String, mappedAuthority: String?) throws -> HTTPControlConfiguration {
        let rules: [HTTPRule]
        switch name {
        case "headers":
            rules = headerRules()
        case "body":
            rules = bodyRules()
        default:
            rules = [try terminalRule(name, mappedAuthority: mappedAuthority)]
        }
        return try HTTPControlConfiguration(
            rules: rules,
            limits: HTTPControlLimits(maximumTasks: name == "orphan" ? 1 : 16),
            breakpointDeadlineSeconds: 30
        )
    }

    private static func terminalRule(_ name: String, mappedAuthority: String?) throws -> HTTPRule {
        switch name {
        case "synthetic":
            return HTTPRule(
                id: "synthetic",
                match: HTTPRuleMatch(phase: .request, path: "/production/respond"),
                action: .respond(
                    status: 201,
                    headers: [HTTPHeaderField(name: "x-synthetic", value: "yes")],
                    body: Data("synthetic".utf8)
                )
            )
        case "breakpoint", "orphan":
            return HTTPRule(
                id: "pause",
                match: HTTPRuleMatch(phase: .request, path: "/production/paused"),
                action: .breakpoint
            )
        case "route", "security-route":
            guard let mappedAuthority else { throw HostError.arguments }
            let path = name == "security-route" ? "/production/security-map" : "/production/mapped"
            return HTTPRule(
                id: "mapped",
                match: HTTPRuleMatch(phase: .request, path: path),
                action: .route(authority: mappedAuthority, sensitiveHeaders: ["x-private"])
            )
        case "delay":
            return HTTPRule(
                id: "delay",
                match: HTTPRuleMatch(phase: .request, path: "/production/delayed"),
                action: .fault(try HTTPFault(direction: .request, delayMilliseconds: 1000))
            )
        default: throw HostError.arguments
        }
    }

    private static func headerRules() -> [HTTPRule] {
        [HTTPControlPhase.request, .response].map { phase in
            HTTPRule(
                id: "headers-\(phase)",
                match: HTTPRuleMatch(phase: phase, path: "/production/headers"),
                action: .editHeaders(remove: ["x-replaced"], append: [
                    HTTPHeaderField(name: "x-text", value: "\u{00FF}"),
                    HTTPHeaderField(name: "x-insert", rawValueBytes: [0x80, 0xFF]),
                    HTTPHeaderField(name: "x-replaced", value: "\u{00FF}"),
                    HTTPHeaderField(name: "x-marker", value: "edited")
                ])
            )
        }
    }

    private static func bodyRules() -> [HTTPRule] {
        [
            HTTPRule(
                id: "request-body",
                match: HTTPRuleMatch(phase: .request, path: "/production/replace-request"),
                action: .replaceBody(
                    Data("edited".utf8),
                    maximumInputBytes: 4,
                    contentEncoding: nil,
                    overrideNoTransform: false
                )
            ),
            HTTPRule(
                id: "response-body",
                match: HTTPRuleMatch(phase: .response, path: "/production/replace-response"),
                action: .replaceBody(
                    Data("replacement".utf8),
                    maximumInputBytes: 13,
                    contentEncoding: nil,
                    overrideNoTransform: false
                )
            )
        ]
    }
}

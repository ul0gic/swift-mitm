import Foundation

extension HARJSONWriter {
    mutating func entry(_ entry: HARExchange, configuration: HARExportConfiguration) async throws {
        guard let measured = entry.timings else { throw HARExportError.missingTimings }
        try await raw("{\"startedDateTime\":")
        try await string(ISO8601DateFormatter().string(from: entry.request.timestamp))
        try await raw(",\"time\":" + String(measured.total) + ",\"request\":")
        try await request(entry, configuration: configuration)
        try await raw(",\"response\":")
        try await response(entry, configuration: configuration)
        try await raw(",\"cache\":{},\"timings\":")
        try await timings(measured)
        try await raw(",\"_swiftmitm\":")
        try await metadata(entry, configuration: configuration)
        try await raw("}")
    }

    private mutating func request(
        _ entry: HARExchange, configuration: HARExportConfiguration
    ) async throws {
        let sourceURL = entry.request.scheme + "://" + entry.request.authority + entry.request.path
        guard let url = redactedURL(sourceURL, configuration: configuration), url.host != nil,
            url.scheme == "http" || url.scheme == "https", let redacted = url.string
        else { throw HARExportError.invalidURL }
        try await raw("{\"method\":")
        try await string(entry.request.method)
        try await raw(",\"url\":")
        try await string(redacted)
        try await raw(",\"httpVersion\":")
        try await string(entry.request.version.rawValue)
        try await raw(",\"cookies\":[],\"headers\":")
        try await fields(entry.request.headers, configuration: configuration)
        try await raw(",\"queryString\":")
        try await query(url.queryItems ?? [])
        try await raw(",\"headersSize\":-1,\"bodySize\":")
        try await raw(String(entry.requestBody?.observedByteCount ?? -1))
        try await postData(entry.requestBody, configuration: configuration)
        try await raw("}")
    }

    private mutating func query(_ items: [URLQueryItem]) async throws {
        try await raw("[")
        for (index, item) in items.enumerated() {
            if index > 0 { try await raw(",") }
            try await raw("{\"name\":")
            try await string(item.name)
            try await raw(",\"value\":")
            try await string(item.value ?? "")
            try await raw("}")
        }
        try await raw("]")
    }

    private mutating func postData(
        _ body: HARBody?, configuration: HARExportConfiguration
    ) async throws {
        guard include(body, configuration: configuration), let body else { return }
        if try validText(body) {
            try await raw(",\"postData\":{\"mimeType\":")
            try await string(body.mimeType)
            try await raw(",\"text\":")
            try await quotedUTF8(body.bytes)
            try await raw("}")
        } else if configuration.includeBinaryRequestExtension {
            try await raw(",\"_swiftmitmPostDataBase64\":")
            try await base64(body.bytes)
        }
    }

    private mutating func response(
        _ entry: HARExchange, configuration: HARExportConfiguration
    ) async throws {
        guard let contentBytes = entry.responseContentBytes else { throw HARExportError.missingContentSize }
        try await raw("{\"status\":" + String(entry.response.status))
        try await raw(",\"statusText\":\"\",\"httpVersion\":")
        try await string(entry.response.version.rawValue)
        try await raw(",\"cookies\":[],\"headers\":")
        try await fields(entry.response.headers, configuration: configuration)
        try await raw(",\"content\":{\"size\":" + String(contentBytes) + ",\"mimeType\":")
        try await string(entry.responseBody?.mimeType ?? "")
        try await content(entry.responseBody, configuration: configuration)
        try await raw("},\"redirectURL\":\"\",\"headersSize\":-1,\"bodySize\":")
        try await raw(String(entry.responseBody?.observedByteCount ?? -1))
        try await raw("}")
    }

    private mutating func content(
        _ body: HARBody?, configuration: HARExportConfiguration
    ) async throws {
        guard include(body, configuration: configuration), let body else { return }
        try await raw(",\"text\":")
        if try validText(body) {
            try await quotedUTF8(body.bytes)
        } else {
            try await base64(body.bytes)
            try await raw(",\"encoding\":\"base64\"")
        }
    }

    private mutating func timings(_ timings: HARExchangeTiming) async throws {
        try await raw("{\"send\":" + String(timings.send))
        try await raw(",\"wait\":" + String(timings.wait))
        try await raw(",\"receive\":" + String(timings.receive))
        let optional = [
            ("blocked", timings.blocked), ("dns", timings.dns),
            ("connect", timings.connect), ("ssl", timings.ssl)
        ]
        for (name, value) in optional {
            if let value { try await raw(",\"" + name + "\":" + String(value)) }
        }
        try await raw("}")
    }

    private mutating func metadata(
        _ entry: HARExchange, configuration: HARExportConfiguration
    ) async throws {
        let requestAllowed = include(entry.requestBody, configuration: configuration)
        let requestText = requestAllowed ? try validText(entry.requestBody) : false
        let requestIncluded = requestAllowed && (requestText || configuration.includeBinaryRequestExtension)
        try await raw("{\"requestID\":")
        try await string(entry.request.id.uuidString)
        try await raw(",\"representation\":")
        try await string(entry.representation == .original ? "original" : "effective")
        try await raw(",\"requestTruncated\":")
        try await raw(entry.requestBody?.truncated == true ? "true" : "false")
        try await raw(",\"responseTruncated\":")
        try await raw(entry.responseBody?.truncated == true ? "true" : "false")
        try await raw(",\"requestBodyIncluded\":")
        try await raw(requestIncluded ? "true" : "false")
        try await raw(",\"responseBodyIncluded\":")
        try await raw(include(entry.responseBody, configuration: configuration) ? "true" : "false")
        try await raw(",\"losses\":")
        try await losses()
        try await raw("}")
    }

    private mutating func losses() async throws {
        let names = [
            "optional-transport-timing-not-inferred", "wire-header-size-unavailable",
            "cookie-views-omitted", "status-reason-unavailable", "redirect-view-omitted",
            "protocol-framing-unavailable", "header-octets-not-preserved",
            "binary-request-uses-extension-or-is-omitted"
        ]
        try await raw("[")
        for (index, name) in names.enumerated() {
            if index > 0 { try await raw(",") }
            try await string(name)
        }
        try await raw("]")
    }
}

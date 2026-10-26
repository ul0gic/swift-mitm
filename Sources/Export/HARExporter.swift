public enum HARExporter {
    public static func export<Entries: AsyncSequence>(
        _ entries: Entries,
        configuration: HARExportConfiguration,
        writer: any HAROutputWriter
    ) async throws where Entries.Element == HARExchange {
        var output = HARJSONWriter(writer: writer)
        try await output.raw("{\"log\":{\"version\":\"1.2\",\"creator\":{")
        try await output.raw("\"name\":\"SwiftMITM\",\"version\":\"0.2.2\"},\"entries\":[")
        var first = true
        for try await entry in entries {
            try Task.checkCancellation()
            try validate(entry, configuration: configuration)
            if !first {
                try await output.raw(",")
            }
            first = false
            try await output.entry(entry, configuration: configuration)
        }
        try await output.raw("]}}")
        try await output.flush()
    }

    private static func validate(_ entry: HARExchange, configuration: HARExportConfiguration) throws {
        guard entry.representation == configuration.representation else {
            throw HARExportError.mismatchedRepresentation
        }
        guard entry.completed, entry.request.id == entry.response.requestID else {
            throw HARExportError.incompleteExchange
        }
        guard entry.timings != nil else { throw HARExportError.missingTimings }
        guard let contentBytes = entry.responseContentBytes else { throw HARExportError.missingContentSize }
        guard contentBytes >= 0 else { throw HARExportError.invalidContentSize }
    }
}

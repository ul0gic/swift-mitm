import Foundation

extension HTTP1MessageParser {
    func parseHead(
        requestProvider: () -> HTTP1RequestMetadata?,
        consumeMethod: () -> Void,
        emit: (HTTP1ParserOutput) -> Void
    ) {
        defer { headBytes.removeAll(keepingCapacity: true) }
        let text = String(bytes: headBytes, encoding: .utf8) ?? ""
        var lines = text.components(separatedBy: "\r\n")
        while lines.last?.isEmpty == true { lines.removeLast() }
        guard let startLine = lines.first, !startLine.isEmpty else {
            fail(emit: emit)
            return
        }
        let headerFields = Self.parseHeaderFields(Array(lines.dropFirst()))

        switch mode {
        case .request:
            parseRequestHead(startLine: startLine, headers: headerFields, emit: emit)
        case .response:
            parseResponseHead(
                startLine: startLine,
                headers: headerFields,
                requestProvider: requestProvider,
                consumeMethod: consumeMethod,
                emit: emit
            )
        }
    }

    private func parseRequestHead(
        startLine: String,
        headers: [HTTPHeaderField],
        emit: (HTTP1ParserOutput) -> Void
    ) {
        let parts = startLine.split(separator: " ", omittingEmptySubsequences: true)
        guard parts.count >= 2 else {
            fail(emit: emit)
            return
        }
        guard let framing = requestFraming(headers: headers) else {
            fail(emit: emit)
            return
        }
        emit(.requestHead(method: String(parts[0]), path: String(parts[1]), headers: headers))
        if Self.isWebSocketUpgrade(headers) {
            emit(.upgradeRequested)
            resetForNextMessage()
            return
        }
        enterBody(framing: framing, emit: emit)
    }

    private func parseResponseHead(
        startLine: String,
        headers: [HTTPHeaderField],
        requestProvider: () -> HTTP1RequestMetadata?,
        consumeMethod: () -> Void,
        emit: (HTTP1ParserOutput) -> Void
    ) {
        let parts = startLine.split(separator: " ", maxSplits: 2, omittingEmptySubsequences: true)
        guard parts.count >= 2, let status = Int(parts[1]) else {
            fail(emit: emit)
            return
        }
        let request = requestProvider()
        if (100..<200).contains(status), status != 101 {
            emit(.responseHead(status: status, headers: headers))
            resetForNextMessage()
            return
        }
        if status == 101, request?.webSocketUpgradeRequested == true, Self.isWebSocketUpgrade(headers) {
            consumeMethod()
            emit(.responseHead(status: status, headers: headers))
            emit(.upgraded)
            phase = .tunnel
            return
        }
        guard let framing = responseFraming(status: status, method: request?.method, headers: headers) else {
            fail(emit: emit)
            return
        }
        consumeMethod()
        emit(.responseHead(status: status, headers: headers))
        enterBody(framing: framing, emit: emit)
    }
}

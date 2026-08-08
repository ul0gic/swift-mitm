import Foundation

enum HTTP1ParserMode: Sendable {
    case request
    case response
}

enum HTTP1ParserOutput: Equatable, Sendable {
    case requestHead(method: String, path: String, headers: [HTTPHeaderField])
    case responseHead(status: Int, headers: [HTTPHeaderField])
    case bodyChunk(byteCount: Int)
    case messageComplete
    /// The message upgraded the connection (WebSocket handshake); all following bytes are opaque tunnel bytes.
    case upgraded
    case failed
}

/// Streaming, observational HTTP/1.1 parser — counts body bytes, never buffers them.
/// Bugs here degrade capture fidelity only; the proxy forwards bytes regardless of parse state.
final class HTTP1MessageParser {
    private enum Phase {
        case head
        case bodyLength(remaining: Int)
        case bodyChunkSize
        case bodyChunkData(remaining: Int)
        case bodyChunkDataTerminator(remaining: Int)
        case bodyChunkTrailer
        case bodyUntilClose
        case tunnel
        case failed
    }

    private static let maxHeadBytes = 256 * 1024

    private let mode: HTTP1ParserMode
    private var phase: Phase = .head
    private var headBytes: [UInt8] = []
    private var lineBytes: [UInt8] = []

    init(mode: HTTP1ParserMode) {
        self.mode = mode
    }

    func feed<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        methodProvider: () -> String?,
        emit: (HTTP1ParserOutput) -> Void,
        bodyBytes: (Bytes.SubSequence) -> Void = { _ in },
        tunnelBytes: (Bytes.SubSequence) -> Void = { _ in }
    ) where Bytes.Element == UInt8, Bytes.Index == Int {
        var index = bytes.startIndex
        let end = bytes.endIndex
        while index < end {
            switch phase {
            case .failed:
                return
            case .tunnel:
                tunnelBytes(bytes[index..<end])
                index = end
            case .head:
                index = consumeHead(bytes, from: index, end: end, methodProvider: methodProvider, emit: emit)
            case .bodyLength(let remaining):
                index = consumeCountedBody(
                    bytes, from: index, end: end, remaining: remaining, emit: emit, bodyBytes: bodyBytes
                )
            case .bodyUntilClose:
                emit(.bodyChunk(byteCount: end - index))
                bodyBytes(bytes[index..<end])
                index = end
            case .bodyChunkSize:
                index = consumeChunkSize(bytes, from: index, end: end, emit: emit)
            case .bodyChunkData(let remaining):
                index = consumeChunkData(
                    bytes, from: index, end: end, remaining: remaining, emit: emit, bodyBytes: bodyBytes
                )
            case .bodyChunkDataTerminator(let remaining):
                index = consumeChunkTerminator(bytes, from: index, end: end, remaining: remaining)
            case .bodyChunkTrailer:
                index = consumeChunkTrailer(bytes, from: index, end: end, emit: emit)
            }
        }
    }

    func finish(emit: (HTTP1ParserOutput) -> Void) {
        if case .bodyUntilClose = phase {
            emit(.messageComplete)
            phase = .head
        }
    }

    private func consumeHead<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        from start: Int,
        end: Int,
        methodProvider: () -> String?,
        emit: (HTTP1ParserOutput) -> Void
    ) -> Int where Bytes.Element == UInt8, Bytes.Index == Int {
        var index = start
        while index < end {
            headBytes.append(bytes[index])
            index += 1
            if headBytes.count > Self.maxHeadBytes {
                fail(emit: emit)
                return end
            }
            if Self.endsWithDoubleCRLF(headBytes) {
                parseHead(methodProvider: methodProvider, emit: emit)
                return index
            }
        }
        return index
    }

    private func consumeCountedBody<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        from start: Int,
        end: Int,
        remaining: Int,
        emit: (HTTP1ParserOutput) -> Void,
        bodyBytes: (Bytes.SubSequence) -> Void
    ) -> Int where Bytes.Element == UInt8, Bytes.Index == Int {
        let take = min(remaining, end - start)
        if take > 0 {
            emit(.bodyChunk(byteCount: take))
            bodyBytes(bytes[start..<(start + take)])
        }
        let left = remaining - take
        if left == 0 {
            emit(.messageComplete)
            resetForNextMessage()
        } else {
            phase = .bodyLength(remaining: left)
        }
        return start + take
    }

    private func consumeChunkSize<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        from start: Int,
        end: Int,
        emit: (HTTP1ParserOutput) -> Void
    ) -> Int where Bytes.Element == UInt8, Bytes.Index == Int {
        var index = start
        while index < end {
            lineBytes.append(bytes[index])
            index += 1
            if Self.endsWithCRLF(lineBytes) {
                guard let size = Self.parseChunkSize(lineBytes) else {
                    fail(emit: emit)
                    return end
                }
                lineBytes.removeAll(keepingCapacity: true)
                phase = size == 0 ? .bodyChunkTrailer : .bodyChunkData(remaining: size)
                return index
            }
        }
        return index
    }

    private func consumeChunkData<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        from start: Int,
        end: Int,
        remaining: Int,
        emit: (HTTP1ParserOutput) -> Void,
        bodyBytes: (Bytes.SubSequence) -> Void
    ) -> Int where Bytes.Element == UInt8, Bytes.Index == Int {
        let take = min(remaining, end - start)
        if take > 0 {
            emit(.bodyChunk(byteCount: take))
            bodyBytes(bytes[start..<(start + take)])
        }
        let left = remaining - take
        phase = left == 0 ? .bodyChunkDataTerminator(remaining: 2) : .bodyChunkData(remaining: left)
        return start + take
    }

    private func consumeChunkTerminator<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        from start: Int,
        end: Int,
        remaining: Int
    ) -> Int where Bytes.Element == UInt8, Bytes.Index == Int {
        let take = min(remaining, end - start)
        let left = remaining - take
        phase = left == 0 ? .bodyChunkSize : .bodyChunkDataTerminator(remaining: left)
        return start + take
    }

    private func consumeChunkTrailer<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        from start: Int,
        end: Int,
        emit: (HTTP1ParserOutput) -> Void
    ) -> Int where Bytes.Element == UInt8, Bytes.Index == Int {
        var index = start
        while index < end {
            lineBytes.append(bytes[index])
            index += 1
            if Self.endsWithCRLF(lineBytes) {
                let isBlankLine = lineBytes.count == 2
                lineBytes.removeAll(keepingCapacity: true)
                if isBlankLine {
                    emit(.messageComplete)
                    resetForNextMessage()
                    return index
                }
            }
        }
        return index
    }

    private func parseHead(methodProvider: () -> String?, emit: (HTTP1ParserOutput) -> Void) {
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
            parseResponseHead(startLine: startLine, headers: headerFields, methodProvider: methodProvider, emit: emit)
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
            emit(.upgraded)
            phase = .tunnel
            return
        }
        enterBody(framing: framing, emit: emit)
    }

    private func parseResponseHead(
        startLine: String,
        headers: [HTTPHeaderField],
        methodProvider: () -> String?,
        emit: (HTTP1ParserOutput) -> Void
    ) {
        let parts = startLine.split(separator: " ", maxSplits: 2, omittingEmptySubsequences: true)
        guard parts.count >= 2, let status = Int(parts[1]) else {
            fail(emit: emit)
            return
        }
        let requestMethod = methodProvider()
        if status == 101, Self.isWebSocketUpgrade(headers) {
            emit(.responseHead(status: status, headers: headers))
            emit(.upgraded)
            phase = .tunnel
            return
        }
        guard let framing = responseFraming(status: status, method: requestMethod, headers: headers) else {
            fail(emit: emit)
            return
        }
        emit(.responseHead(status: status, headers: headers))
        enterBody(framing: framing, emit: emit)
    }

    private func resetForNextMessage() {
        phase = .head
        headBytes.removeAll(keepingCapacity: true)
        lineBytes.removeAll(keepingCapacity: true)
    }

    private func fail(emit: (HTTP1ParserOutput) -> Void) {
        phase = .failed
        emit(.failed)
    }
}

// MARK: - Body Framing

private extension HTTP1MessageParser {
    enum BodyFraming {
        case none
        case length(Int)
        case chunked
        case untilClose
    }

    func enterBody(framing: BodyFraming, emit: (HTTP1ParserOutput) -> Void) {
        switch framing {
        case .none:
            emit(.messageComplete)
            resetForNextMessage()
        case .length(let count) where count <= 0:
            emit(.messageComplete)
            resetForNextMessage()
        case .length(let count):
            phase = .bodyLength(remaining: count)
        case .chunked:
            phase = .bodyChunkSize
        case .untilClose:
            phase = .bodyUntilClose
        }
    }

    func requestFraming(headers: [HTTPHeaderField]) -> BodyFraming? {
        let chunked = Self.isChunked(headers)
        switch Self.contentLength(headers) {
        case .invalid:
            return nil
        case .none:
            return chunked ? .chunked : BodyFraming.none
        case .value(let length):
            return chunked ? .chunked : .length(length)
        }
    }

    func responseFraming(status: Int, method: String?, headers: [HTTPHeaderField]) -> BodyFraming? {
        if method?.uppercased() == "HEAD" { return BodyFraming.none }
        if (100..<200).contains(status) || status == 204 || status == 304 { return BodyFraming.none }
        let chunked = Self.isChunked(headers)
        switch Self.contentLength(headers) {
        case .invalid:
            return nil
        case .none:
            return chunked ? .chunked : .untilClose
        case .value(let length):
            return chunked ? .chunked : .length(length)
        }
    }
}

import Foundation

enum HTTP1ParserMode: Sendable {
    case request
    case response
}

struct HTTP1RequestMetadata {
    let method: String
    let webSocketUpgradeRequested: Bool
}

enum HTTP1ParserOutput: Equatable, Sendable {
    case requestHead(method: String, path: String, headers: [HTTPHeaderField])
    case responseHead(status: Int, headers: [HTTPHeaderField])
    case bodyChunk(byteCount: Int)
    case trailers([HTTPHeaderField])
    case messageComplete
    case upgradeRequested
    case upgraded
    case failed
}

final class HTTP1MessageParser {
    enum Phase {
        case head
        case bodyLength(remaining: Int)
        case bodyChunkSize
        case bodyChunkData(remaining: Int)
        case bodyChunkDataTerminator(matched: Int)
        case bodyChunkTrailer
        case bodyUntilClose
        case tunnel
        case failed
    }

    private static let maxHeadBytes = 256 * 1024
    private static let maxChunkSizeLineBytes = 8 * 1024
    private static let maxTrailerBytes = 256 * 1024

    let mode: HTTP1ParserMode
    var phase: Phase = .head
    var headBytes: [UInt8] = []
    private var lineBytes: [UInt8] = []
    private var trailerLines: [String] = []
    private var trailerByteCount = 0

    init(mode: HTTP1ParserMode) {
        self.mode = mode
    }

    func feed<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        requestProvider: () -> HTTP1RequestMetadata?,
        consumeMethod: () -> Void = {},
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
                index = consumeHead(
                    bytes,
                    from: index,
                    end: end,
                    requestProvider: requestProvider,
                    consumeMethod: consumeMethod,
                    emit: emit
                )
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
            case .bodyChunkDataTerminator(let matched):
                index = consumeChunkTerminator(bytes, from: index, end: end, matched: matched, emit: emit)
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
        requestProvider: () -> HTTP1RequestMetadata?,
        consumeMethod: () -> Void,
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
                parseHead(
                    requestProvider: requestProvider,
                    consumeMethod: consumeMethod,
                    emit: emit
                )
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
            if lineBytes.count > Self.maxChunkSizeLineBytes {
                fail(emit: emit)
                return end
            }
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
        phase = left == 0 ? .bodyChunkDataTerminator(matched: 0) : .bodyChunkData(remaining: left)
        return start + take
    }

    private func consumeChunkTerminator<Bytes: RandomAccessCollection>(
        _ bytes: Bytes,
        from start: Int,
        end: Int,
        matched: Int,
        emit: (HTTP1ParserOutput) -> Void
    ) -> Int where Bytes.Element == UInt8, Bytes.Index == Int {
        var index = start
        var matched = matched
        let terminator: [UInt8] = [13, 10]
        while index < end, matched < terminator.count {
            guard bytes[index] == terminator[matched] else {
                fail(emit: emit)
                return end
            }
            index += 1
            matched += 1
        }
        phase = matched == terminator.count ? .bodyChunkSize : .bodyChunkDataTerminator(matched: matched)
        return index
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
            trailerByteCount += 1
            index += 1
            if trailerByteCount > Self.maxTrailerBytes {
                fail(emit: emit)
                return end
            }
            if Self.endsWithCRLF(lineBytes) {
                let isBlankLine = lineBytes.count == 2
                if !isBlankLine {
                    guard let line = String(bytes: lineBytes.dropLast(2), encoding: .utf8) else {
                        fail(emit: emit)
                        return end
                    }
                    trailerLines.append(line)
                }
                lineBytes.removeAll(keepingCapacity: true)
                if isBlankLine {
                    let trailers = Self.parseHeaderFields(trailerLines)
                    if !trailers.isEmpty {
                        emit(.trailers(trailers))
                    }
                    emit(.messageComplete)
                    resetForNextMessage()
                    return index
                }
            }
        }
        return index
    }

    func resetForNextMessage() {
        phase = .head
        headBytes.removeAll(keepingCapacity: true)
        lineBytes.removeAll(keepingCapacity: true)
        trailerLines.removeAll(keepingCapacity: true)
        trailerByteCount = 0
    }

    func fail(emit: (HTTP1ParserOutput) -> Void) {
        phase = .failed
        emit(.failed)
    }
}

extension HTTP1MessageParser {
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
        if method?.uppercased() == "HEAD" {
            return BodyFraming.none
        }
        if (100..<200).contains(status) || status == 204 || status == 304 {
            return BodyFraming.none
        }
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

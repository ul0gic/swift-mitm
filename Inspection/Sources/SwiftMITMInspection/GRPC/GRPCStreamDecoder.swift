import Foundation

public final class GRPCStreamDecoder {
    private let format: GRPCWireFormat
    private let maximumMessageBytes: Int
    private let decodeMessage: (([UInt8]) throws -> [UInt8])?
    private let compressionEncoding: String?
    private let emit: (GRPCStreamEvent) -> Void
    private let budget: InspectionMemoryBudget
    private let expectsStatus: Bool
    private let initialMetadata: [(String, String)]
    private let httpStatus: Int?
    private var reservation = 0
    private var header: [UInt8] = []
    private var payload: [UInt8] = []
    private var quartet: [UInt8] = []
    private var expected = 0
    private var offset: UInt64 = 0
    private var transportOffset: UInt64 = 0
    private var frameStart: UInt64 = 0
    private var ordinal: UInt64 = 0
    private var trailerSeen = false
    private var terminal = false
    private var webTrailers: [(String, String)] = []
    private var compressedSession: GRPCMessageDecompressionSession?
    private var quartetOutput: [UInt8] = []
    public var hasPendingWork: Bool { compressedSession != nil || !quartetOutput.isEmpty }

    public init(
        format: GRPCWireFormat,
        budget: InspectionMemoryBudget,
        maximumMessageBytes: Int = 4 * 1024 * 1024,
        expectsStatus: Bool = true,
        initialMetadata: [(String, String)] = [],
        httpStatus: Int? = nil,
        compressionEncoding: String? = nil,
        decodeMessage: (([UInt8]) throws -> [UInt8])? = nil,
        emit: @escaping (GRPCStreamEvent) -> Void
    ) {
        self.format = format
        self.budget = budget
        self.expectsStatus = expectsStatus
        self.initialMetadata = initialMetadata
        self.httpStatus = httpStatus
        self.maximumMessageBytes = min(max(0, maximumMessageBytes), 16 * 1024 * 1024)
        self.decodeMessage = decodeMessage
        self.compressionEncoding = compressionEncoding
        self.emit = emit
    }

    deinit { budget.release(reservation) }

    public func feed(_ bytes: [UInt8]) {
        bytes.withUnsafeBytes { input in
            var offset = 0
            repeat {
                let progress = feedResumable(UnsafeRawBufferPointer(rebasing: input[offset...]))
                offset += progress.consumedBytes
                if terminal {
                    break
                }
            } while offset < input.count || hasPendingWork
        }
    }

    public func feedResumable(
        _ bytes: UnsafeRawBufferPointer, maximumOutputBytes: Int = 1_048_576
    ) -> GRPCDecoderProgress {
        guard (1...1_048_576).contains(maximumOutputBytes) else {
            fail(.limitExceeded)
            return GRPCDecoderProgress(consumedBytes: 0, producedBytes: 0, outputLimit: false)
        }
        var consumed = 0
        var produced = 0
        while !terminal {
            if let session = compressedSession {
                produced += resumeCompression(session, maximumOutputBytes: maximumOutputBytes - produced)
                if produced == maximumOutputBytes {
                    return GRPCDecoderProgress(consumedBytes: consumed, producedBytes: produced, outputLimit: true)
                }
                continue
            }
            if !quartetOutput.isEmpty {
                consume(quartetOutput.removeFirst())
                continue
            }
            guard consumed < bytes.count else { break }
            let byte = bytes[consumed]
            consumed += 1
            transportOffset += 1
            consumeWireByte(byte)
        }
        return GRPCDecoderProgress(consumedBytes: consumed, producedBytes: produced, outputLimit: false)
    }

    public func finish(trailers: [(String, String)] = [], transportComplete: Bool = true) {
        guard !terminal else { return }
        if !transportComplete || !quartet.isEmpty || !header.isEmpty || hasPendingWork {
            fail(.incompleteTransport)
            return
        }
        terminal = true
        guard expectsStatus else {
            budget.release(reservation)
            reservation = 0
            return
        }
        let metadata = initialMetadata + webTrailers + trailers
        let statuses = metadata.filter { $0.0.lowercased() == "grpc-status" }.map(\.1)
        if statuses.isEmpty {
            emit(.diagnostic(.missingStatus))
        }
        if statuses.count > 1 {
            emit(.diagnostic(.duplicateStatus))
        }
        let code = (statuses.count == 1 ? statuses.first : nil).flatMap { value -> Int? in
            guard !value.isEmpty, value.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }),
                  let result = Int(value), (0...16).contains(result) else { return nil }
            return result
        }
        if statuses.count == 1 && code == nil {
            emit(.diagnostic(.invalidStatus))
        }
        let raw = metadata.first { $0.0.lowercased() == "grpc-message" }?.1
        emit(.status(GRPCStatus(
            httpStatus: httpStatus,
            code: code,
            rawMessage: raw,
            message: raw.map(Self.percentDecode),
            details: metadata.first { $0.0.lowercased() == "grpc-status-details-bin" }?.1,
            metadata: metadata
        )))
        webTrailers.removeAll()
        budget.release(reservation)
        reservation = 0
    }
}

private extension GRPCStreamDecoder {
    func resumeCompression(
        _ session: GRPCMessageDecompressionSession, maximumOutputBytes: Int
    ) -> Int {
        do {
            let result = try session.turn(maximumOutputBytes: maximumOutputBytes)
            if result.complete {
                completeFrame(decoded: session.bytes)
                compressedSession = nil
            }
            return result.produced
        } catch let diagnostic as GRPCDiagnostic {
            fail(diagnostic)
        } catch {
            fail(.malformedEnvelope)
        }
        return 0
    }

    func consumeWireByte(_ byte: UInt8) {
        guard format == .webText else {
            consume(byte)
            return
        }
        quartet.append(byte)
        if quartet.count == 4 {
            let values = quartet
            quartet.removeAll(keepingCapacity: true)
            guard let decoded = decodeQuartet(values) else {
                fail(.invalidBase64)
                return
            }
            quartetOutput = decoded
        }
    }

    func consume(_ byte: UInt8) {
        guard !terminal else { return }
        guard !trailerSeen else {
            fail(.invalidTrailers)
            return
        }
        if header.count < 5 {
            if header.isEmpty {
                frameStart = transportOffset - (format == .webText ? 4 : 1)
            }
            header.append(byte)
            if header.count == 5 {
                beginFrame()
            }
        } else {
            payload.append(byte)
            if payload.count == expected {
                completeFrame()
            }
        }
    }

    func beginFrame() {
        let flag = header[0]
        guard flag == 0 || flag == 1 || (format != .native && (flag == 128 || flag == 129)) else {
            fail(.malformedEnvelope)
            return
        }
        expected = header.dropFirst().reduce(0) { ($0 << 8) | Int($1) }
        guard expected <= maximumMessageBytes else {
            fail(.limitExceeded)
            return
        }
        let charge = expected * 3 + (flag & 1 != 0 && compressionEncoding == nil ? maximumMessageBytes * 2 : 0)
        guard budget.reserve(charge) else {
            fail(.limitExceeded)
            return
        }
        reservation += charge
        payload.reserveCapacity(expected)
        if expected == 0 {
            completeFrame()
        }
    }

    func completeFrame(decoded: [UInt8]? = nil) {
        let flag = header[0]
        let compressed = flag & 1 != 0
        var bytes = decoded ?? payload
        if compressed && decoded == nil, let compressionEncoding {
            do {
                compressedSession = try GRPCMessageDecompressionSession(
                    payload, encoding: compressionEncoding, maximum: maximumMessageBytes, budget: budget
                )
            } catch let diagnostic as GRPCDiagnostic { fail(diagnostic) }
            catch { fail(.malformedEnvelope) }
            return
        }
        if compressed && decoded == nil {
            guard let decoded = legacyMessage() else { return }
            bytes = decoded
        }
        if flag & 128 != 0 {
            guard parseTrailers(bytes) else { return }
            trailerSeen = true
        } else {
            emit(.message(GRPCMessage(
                ordinal: ordinal,
                envelopeOffset: offset,
                encodedRange: frameStart..<transportOffset,
                compressed: compressed,
                bytes: bytes
            )))
            ordinal += 1
        }
        offset += UInt64(expected) + 5
        header.removeAll(keepingCapacity: true)
        payload.removeAll(keepingCapacity: false)
        if !trailerSeen {
            budget.release(reservation)
            reservation = 0
        }
    }

    func legacyMessage() -> [UInt8]? {
        guard let decodeMessage else {
            fail(.unsupportedCompression)
            return nil
        }
        do {
            let bytes = try decodeMessage(payload)
            guard bytes.count <= maximumMessageBytes else {
                fail(.limitExceeded)
                return nil
            }
            return bytes
        } catch let diagnostic as GRPCDiagnostic {
            fail(diagnostic)
        } catch {
            fail(.malformedEnvelope)
        }
        return nil
    }

    func parseTrailers(_ bytes: [UInt8]) -> Bool {
        guard budget.reserve(bytes.count * 12) else {
            fail(.limitExceeded)
            return false
        }
        reservation += bytes.count * 12
        guard let text = String(bytes: bytes, encoding: .utf8), text.isEmpty || text.hasSuffix("\r\n") else {
            fail(.invalidTrailers)
            return false
        }
        for line in text.components(separatedBy: "\r\n").dropLast() {
            guard let colon = line.firstIndex(of: ":"), colon != line.startIndex else {
                fail(.invalidTrailers)
                return false
            }
            let name = String(line[..<colon])
            guard name.utf8.allSatisfy({
                ($0 >= 97 && $0 <= 122) || ($0 >= 48 && $0 <= 57) || $0 == 45 || $0 == 46 || $0 == 95
            }) else {
                fail(.invalidTrailers)
                return false
            }
            let value = String(line[line.index(after: colon)...]).trimmingCharacters(in: .whitespaces)
            guard value.utf8.allSatisfy({ $0 == 9 || ($0 >= 32 && $0 <= 126) }) else {
                fail(.invalidTrailers)
                return false
            }
            webTrailers.append((name, value))
        }
        return true
    }

    func fail(_ diagnostic: GRPCDiagnostic) {
        terminal = true
        header.removeAll()
        payload.removeAll()
        quartet.removeAll()
        webTrailers.removeAll()
        compressedSession = nil
        quartetOutput = []
        budget.release(reservation)
        reservation = 0
        emit(.diagnostic(diagnostic))
    }

    func decodeQuartet(_ bytes: [UInt8]) -> [UInt8]? {
        guard let first = Self.base64Value(bytes[0]), let second = Self.base64Value(bytes[1]) else { return nil }
        if bytes[2] == 61 {
            guard bytes[3] == 61, second & 15 == 0 else { return nil }
            return [(first << 2) | (second >> 4)]
        }
        guard let third = Self.base64Value(bytes[2]) else { return nil }
        if bytes[3] == 61 {
            guard third & 3 == 0 else { return nil }
            return [(first << 2) | (second >> 4), (second << 4) | (third >> 2)]
        }
        guard let fourth = Self.base64Value(bytes[3]) else { return nil }
        return [(first << 2) | (second >> 4), (second << 4) | (third >> 2), (third << 6) | fourth]
    }

    static func base64Value(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 65...90: byte - 65
        case 97...122: byte - 71
        case 48...57: byte + 4
        case 43: 62
        case 47: 63
        default: nil
        }
    }

    static func percentDecode(_ text: String) -> String {
        let input = Array(text.utf8)
        var output: [UInt8] = []
        var index = 0
        while index < input.count {
            if input[index] == 37, index + 2 < input.count,
               let byte = UInt8(String(decoding: input[(index + 1)...(index + 2)], as: UTF8.self), radix: 16) {
                output.append(byte)
                index += 3
            } else {
                output.append(input[index])
                index += 1
            }
        }
        return String(decoding: output, as: UTF8.self)
    }
}

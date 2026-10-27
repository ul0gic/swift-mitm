public enum GRPCMessageDecompression {
    public static func decode(
        _ bytes: [UInt8],
        encoding: String,
        maximumDecodedBytes: Int,
        budget: InspectionMemoryBudget
    ) throws -> [UInt8] {
        let coding: ContentCoding
        switch encoding.lowercased() {
        case "gzip": coding = .gzip
        case "deflate": coding = .deflate
        default: throw GRPCDiagnostic.unsupportedCompression
        }
        guard (0...(16 * 1024 * 1024)).contains(maximumDecodedBytes),
              budget.reserve(maximumDecodedBytes * 2) else { throw GRPCDiagnostic.limitExceeded }
        defer { budget.release(maximumDecodedBytes * 2) }
        let decoder = try ContentDecoder(coding: coding, budget: budget)
        var output: [UInt8] = []
        func collect(_ slice: UnsafeRawBufferPointer) throws {
            guard slice.count <= maximumDecodedBytes - output.count else { throw GRPCDiagnostic.limitExceeded }
            output.append(contentsOf: slice)
        }
        try feedInput(bytes, decoder: decoder, emit: collect)
        while true {
            let progress = try decoder.finish(maximumOutputBytes: 1024 * 1024, emit: collect)
            if progress.state == .complete {
                return output
            }
            guard progress.producedBytes > 0 else { throw GRPCDiagnostic.malformedEnvelope }
        }
    }

    private static func feedInput(
        _ bytes: [UInt8],
        decoder: ContentDecoder,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws {
        try bytes.withUnsafeBytes { input in
            var consumed = 0
            while consumed < input.count {
                let end = min(input.count, consumed + 1024 * 1024)
                let progress = try decoder.feed(
                    UnsafeRawBufferPointer(rebasing: input[consumed..<end]),
                    maximumOutputBytes: 1024 * 1024,
                    emit: emit
                )
                consumed += progress.consumedBytes
                guard progress.consumedBytes > 0 || progress.producedBytes > 0 else {
                    throw GRPCDiagnostic.malformedEnvelope
                }
            }
        }
    }
}

final class GRPCMessageDecompressionSession {
    private let encoded: [UInt8]
    private let decoder: ContentDecoder
    private let maximum: Int
    private let budget: InspectionMemoryBudget
    private var offset = 0
    private(set) var bytes: [UInt8] = []

    init(_ encoded: [UInt8], encoding: String, maximum: Int, budget: InspectionMemoryBudget) throws {
        let coding: ContentCoding
        switch encoding.lowercased() {
        case "gzip": coding = .gzip
        case "deflate": coding = .deflate
        default: throw GRPCDiagnostic.unsupportedCompression
        }
        decoder = try ContentDecoder(coding: coding, budget: budget)
        guard budget.reserve(maximum * 2) else { throw GRPCDiagnostic.limitExceeded }
        self.encoded = encoded
        self.maximum = maximum
        self.budget = budget
    }

    deinit { budget.release(maximum * 2) }

    func turn(maximumOutputBytes: Int) throws -> (produced: Int, complete: Bool) {
        var produced = 0
        while produced < maximumOutputBytes {
            let progress: ContentDecoderProgress
            if offset < encoded.count {
                progress = try encoded.withUnsafeBytes { input in
                    let end = min(input.count, offset + 1_048_576)
                    return try decoder.feed(
                        UnsafeRawBufferPointer(rebasing: input[offset..<end]),
                        maximumOutputBytes: maximumOutputBytes - produced,
                        emit: collect
                    )
                }
                offset += progress.consumedBytes
            } else {
                progress = try decoder.finish(maximumOutputBytes: maximumOutputBytes - produced, emit: collect)
                if case .complete = progress.state {
                    return (produced + progress.producedBytes, true)
                }
            }
            produced += progress.producedBytes
            guard progress.consumedBytes > 0 || progress.producedBytes > 0 else {
                throw GRPCDiagnostic.malformedEnvelope
            }
        }
        return (produced, false)
    }

    private func collect(_ slice: UnsafeRawBufferPointer) throws {
        guard slice.count <= maximum - bytes.count else { throw GRPCDiagnostic.limitExceeded }
        bytes.append(contentsOf: slice)
    }
}

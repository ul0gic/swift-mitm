import Compression
import zlib

public enum ContentCoding: Sendable {
    case gzip
    case deflate
    case brotli
}

public enum ContentDecoderError: Error, Sendable {
    case invalidConfiguration
    case initializationFailed
    case malformed
    case truncated
    case trailingBytes
    case alreadyFinished
}

public struct ContentDecoderProgress: Sendable {
    public enum State: Sendable {
        case needsInput
        case outputLimit
        case complete
    }

    public let consumedBytes: Int
    public let producedBytes: Int
    public let state: State
}

public final class ContentDecoder {
    public let reservedStorageBytes: Int
    private let budget: InspectionMemoryBudget
    private let coding: ContentCoding
    private let sliceBytes: Int
    private let output: UnsafeMutablePointer<UInt8>
    private var zstream: UnsafeMutablePointer<z_stream>?
    private var appleStream: compression_stream
    private var initializedApple = false
    private var ended = false
    private var finished = false
    private var failed = false

    public init(
        coding: ContentCoding,
        budget: InspectionMemoryBudget,
        outputSliceBytes: Int = 65_536
    ) throws {
        guard outputSliceBytes > 0, outputSliceBytes <= 65_536 else {
            throw ContentDecoderError.invalidConfiguration
        }
        guard budget.reserve(outputSliceBytes) else { throw InspectionBudgetError.capacityExceeded }
        self.budget = budget
        reservedStorageBytes = outputSliceBytes
        self.coding = coding
        sliceBytes = outputSliceBytes
        output = .allocate(capacity: outputSliceBytes)
        appleStream = compression_stream(
            dst_ptr: output,
            dst_size: 0,
            src_ptr: output,
            src_size: 0,
            state: nil
        )
        if coding == .brotli {
            guard compression_stream_init(&appleStream, COMPRESSION_STREAM_DECODE, COMPRESSION_BROTLI)
                == COMPRESSION_STATUS_OK else {
                throw ContentDecoderError.initializationFailed
            }
            initializedApple = true
        } else {
            let stream = UnsafeMutablePointer<z_stream>.allocate(capacity: 1)
            stream.initialize(to: z_stream())
            let status = inflateInit2_(
                stream,
                coding == .gzip ? 31 : 15,
                ZLIB_VERSION,
                Int32(MemoryLayout<z_stream>.size)
            )
            guard status == Z_OK else {
                stream.deinitialize(count: 1)
                stream.deallocate()
                throw ContentDecoderError.initializationFailed
            }
            zstream = stream
        }
    }

    deinit {
        if let zstream {
            _ = inflateEnd(zstream)
            zstream.deinitialize(count: 1)
            zstream.deallocate()
        }
        if initializedApple {
            _ = compression_stream_destroy(&appleStream)
        }
        output.deallocate()
        budget.release(reservedStorageBytes)
    }

    public func feed(
        _ input: UnsafeRawBufferPointer,
        maximumOutputBytes: Int,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws -> ContentDecoderProgress {
        try process(input, maximumOutputBytes: maximumOutputBytes, final: false, emit: emit)
    }

    public func finish(
        maximumOutputBytes: Int,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws -> ContentDecoderProgress {
        try process(
            UnsafeRawBufferPointer(start: nil, count: 0),
            maximumOutputBytes: maximumOutputBytes,
            final: true,
            emit: emit
        )
    }

    private func process(
        _ input: UnsafeRawBufferPointer,
        maximumOutputBytes: Int,
        final: Bool,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws -> ContentDecoderProgress {
        guard !finished, !failed else { throw ContentDecoderError.alreadyFinished }
        guard maximumOutputBytes > 0, maximumOutputBytes <= 1_048_576,
              input.count <= 1_048_576 else {
            throw ContentDecoderError.invalidConfiguration
        }
        do {
            return try decode(input, budget: maximumOutputBytes, final: final, emit: emit)
        } catch {
            failed = true
            throw error
        }
    }

    private func decode(
        _ input: UnsafeRawBufferPointer,
        budget: Int,
        final: Bool,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws -> ContentDecoderProgress {
        var consumed = 0
        var produced = 0
        defer { clearPointers() }
        while produced < budget {
            if ended, consumed == input.count {
                if final {
                    finished = true
                }
                return ContentDecoderProgress(
                    consumedBytes: consumed,
                    producedBytes: produced,
                    state: final || coding != .gzip ? .complete : .needsInput
                )
            }
            if ended {
                try beginNextMember()
            }
            let capacity = min(sliceBytes, budget - produced)
            let available = min(input.count - consumed, Int(UInt32.max))
            let source = UnsafeRawBufferPointer(rebasing: input[consumed..<(consumed + available)])
            let (used, written) = try nativeStep(source, capacity: capacity, final: final)
            consumed += used
            produced += written
            if written > 0 {
                try emit(UnsafeRawBufferPointer(start: output, count: written))
            }
            if ended {
                continue
            }
            if try needsInput((used, written), consumed: consumed, inputCount: input.count, final: final) {
                return ContentDecoderProgress(
                    consumedBytes: consumed,
                    producedBytes: produced,
                    state: .needsInput
                )
            }
            if consumed == input.count && written < capacity && !final {
                return ContentDecoderProgress(
                    consumedBytes: consumed,
                    producedBytes: produced,
                    state: .needsInput
                )
            }
        }
        return ContentDecoderProgress(
            consumedBytes: consumed,
            producedBytes: produced,
            state: .outputLimit
        )
    }

    private func needsInput(
        _ progress: (used: Int, written: Int),
        consumed: Int,
        inputCount: Int,
        final: Bool
    ) throws -> Bool {
        guard progress.used == 0 && progress.written == 0 else { return false }
        if final {
            throw ContentDecoderError.truncated
        }
        if consumed != inputCount {
            throw ContentDecoderError.malformed
        }
        return true
    }

    private func beginNextMember() throws {
        guard coding == .gzip, let zstream else { throw ContentDecoderError.trailingBytes }
        guard inflateReset(zstream) == Z_OK else { throw ContentDecoderError.malformed }
        ended = false
    }

    private func clearPointers() {
        if let zstream {
            zstream.pointee.next_in = nil
            zstream.pointee.avail_in = 0
            zstream.pointee.next_out = nil
            zstream.pointee.avail_out = 0
        }
        appleStream.src_ptr = UnsafePointer(output)
        appleStream.src_size = 0
        appleStream.dst_ptr = output
        appleStream.dst_size = 0
    }

    private func nativeStep(
        _ input: UnsafeRawBufferPointer,
        capacity: Int,
        final: Bool
    ) throws -> (used: Int, written: Int) {
        let source = input.baseAddress?.assumingMemoryBound(to: UInt8.self)
        if let zstream {
            zstream.pointee.next_in = source.map { UnsafeMutablePointer(mutating: $0) }
            zstream.pointee.avail_in = UInt32(input.count)
            zstream.pointee.next_out = output
            zstream.pointee.avail_out = UInt32(capacity)
            let status = inflate(zstream, final ? Z_FINISH : Z_NO_FLUSH)
            guard status == Z_OK || status == Z_STREAM_END || status == Z_BUF_ERROR else {
                throw ContentDecoderError.malformed
            }
            ended = status == Z_STREAM_END
            return (input.count - Int(zstream.pointee.avail_in), capacity - Int(zstream.pointee.avail_out))
        }
        appleStream.src_ptr = source ?? UnsafePointer(output)
        appleStream.src_size = input.count
        appleStream.dst_ptr = output
        appleStream.dst_size = capacity
        let status = compression_stream_process(
            &appleStream,
            final ? Int32(COMPRESSION_STREAM_FINALIZE.rawValue) : 0
        )
        guard status != COMPRESSION_STATUS_ERROR else { throw ContentDecoderError.malformed }
        ended = status == COMPRESSION_STATUS_END
        return (input.count - appleStream.src_size, capacity - appleStream.dst_size)
    }
}

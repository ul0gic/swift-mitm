import Darwin
import zlib

struct WebSocketInflationProgress {
    let consumed: Int
    let produced: Int
    let outputLimit: Bool
}

private final class WebSocketAllocationOwner {
    let budget: InspectionMemoryBudget
    init(_ budget: InspectionMemoryBudget) { self.budget = budget }
}

final class WebSocketInflater {
    private let owner: WebSocketAllocationOwner
    private let stream: UnsafeMutablePointer<z_stream>
    private let output: UnsafeMutablePointer<UInt8>
    private static let storageBytes = MemoryLayout<z_stream>.stride + 65_536
    private var initialized = false

    init(windowBits: Int32, budget: InspectionMemoryBudget) throws {
        guard budget.reserve(Self.storageBytes) else { throw InspectionBudgetError.capacityExceeded }
        owner = WebSocketAllocationOwner(budget)
        stream = .allocate(capacity: 1)
        stream.initialize(to: z_stream())
        output = .allocate(capacity: 65_536)
        stream.pointee.opaque = Unmanaged.passUnretained(owner).toOpaque()
        stream.pointee.zalloc = { opaque, items, size in
            guard let opaque else { return nil }
            let owner = Unmanaged<WebSocketAllocationOwner>.fromOpaque(opaque).takeUnretainedValue()
            let (payload, overflow) = Int(items).multipliedReportingOverflow(by: Int(size))
            let (total, totalOverflow) = payload.addingReportingOverflow(16)
            guard !overflow, !totalOverflow, owner.budget.reserve(total) else { return nil }
            guard let allocation = calloc(1, total) else {
                owner.budget.release(total)
                return nil
            }
            allocation.storeBytes(of: total, as: Int.self)
            return allocation.advanced(by: 16)
        }
        stream.pointee.zfree = { opaque, address in
            guard let opaque, let address else { return }
            let owner = Unmanaged<WebSocketAllocationOwner>.fromOpaque(opaque).takeUnretainedValue()
            let allocation = address.advanced(by: -16)
            let total = allocation.load(as: Int.self)
            free(allocation)
            owner.budget.release(total)
        }
        let status = inflateInit2_(stream, -windowBits, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size))
        guard status == Z_OK else {
            stream.deinitialize(count: 1)
            stream.deallocate()
            output.deallocate()
            budget.release(Self.storageBytes)
            if status == Z_MEM_ERROR {
                throw InspectionBudgetError.capacityExceeded
            }
            throw WebSocketMessageError.malformedCompression
        }
        initialized = true
    }

    deinit {
        guard initialized else { return }
        _ = inflateEnd(stream)
        stream.deinitialize(count: 1)
        stream.deallocate()
        output.deallocate()
        owner.budget.release(Self.storageBytes)
    }

    var atBoundary: Bool { stream.pointee.data_type & 128 != 0 }

    func feed(
        _ bytes: UnsafeRawBufferPointer,
        maximumOutputBytes: Int,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws -> WebSocketInflationProgress {
        guard bytes.count <= Int(UInt32.max) else { throw WebSocketMessageError.messageLimit }
        stream.pointee.next_in = bytes.baseAddress.map {
            UnsafeMutablePointer(mutating: $0.assumingMemoryBound(to: UInt8.self))
        }
        stream.pointee.avail_in = UInt32(bytes.count)
        defer {
            stream.pointee.next_in = nil
            stream.pointee.next_out = nil
            stream.pointee.avail_in = 0
            stream.pointee.avail_out = 0
        }
        var produced = 0
        repeat {
            let capacity = min(65_536, maximumOutputBytes - produced)
            stream.pointee.next_out = output
            stream.pointee.avail_out = UInt32(capacity)
            let before = stream.pointee.avail_in
            let status = inflate(stream, Z_BLOCK)
            guard status == Z_OK || status == Z_BUF_ERROR else {
                if status == Z_MEM_ERROR {
                    throw InspectionBudgetError.capacityExceeded
                }
                throw WebSocketMessageError.malformedCompression
            }
            let written = capacity - Int(stream.pointee.avail_out)
            produced += written
            if written > 0 {
                try emit(UnsafeRawBufferPointer(start: output, count: written))
            }
            if before == stream.pointee.avail_in, written == 0 {
                guard stream.pointee.avail_in == 0 else { throw WebSocketMessageError.malformedCompression }
                return WebSocketInflationProgress(
                    consumed: bytes.count - Int(stream.pointee.avail_in), produced: produced, outputLimit: false
                )
            }
            if produced == maximumOutputBytes {
                return WebSocketInflationProgress(
                    consumed: bytes.count - Int(stream.pointee.avail_in), produced: produced, outputLimit: true
                )
            }
        } while stream.pointee.avail_in > 0 || stream.pointee.avail_out == 0
        return WebSocketInflationProgress(
            consumed: bytes.count - Int(stream.pointee.avail_in), produced: produced, outputLimit: false
        )
    }
}

import Darwin
import Foundation

enum MachMemoryError: Error, Equatable, LocalizedError, Sendable {
    case samplingUnavailable(kern_return_t)

    var errorDescription: String? {
        switch self {
        case .samplingUnavailable(let result):
            "RSS sampling unavailable: task_info returned \(result)"
        }
    }
}

enum MachMemory {
    static func residentBytes() throws(MachMemoryError) -> UInt64 {
        try residentBytesResult().get()
    }

    static func residentBytesResult() -> Result<UInt64, MachMemoryError> {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(
            MemoryLayout<mach_task_basic_info>.size / MemoryLayout<natural_t>.size
        )
        let result = withUnsafeMutablePointer(to: &info) { pointer in
            pointer.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { rebound in
                task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), rebound, &count)
            }
        }
        return residentBytesResult(result: result, sampledBytes: info.resident_size)
    }

    static func residentBytes(result: kern_return_t, sampledBytes: UInt64) throws(MachMemoryError) -> UInt64 {
        try residentBytesResult(result: result, sampledBytes: sampledBytes).get()
    }

    private static func residentBytesResult(
        result: kern_return_t,
        sampledBytes: UInt64
    ) -> Result<UInt64, MachMemoryError> {
        result == KERN_SUCCESS ? .success(sampledBytes) : .failure(.samplingUnavailable(result))
    }

    static func growth(from baseline: UInt64, to peak: UInt64) -> UInt64 {
        peak > baseline ? peak - baseline : 0
    }
}

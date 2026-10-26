import NIOConcurrencyHelpers
import SwiftMITM

public final class InspectionMemoryBudget: Sendable {
    public let maximumBytes: Int
    public let admission: ResourceAdmissionController?
    private let account: ResourceRetainedByteAccount?
    private let used = NIOLockedValueBox(0)

    public init(
        maximumBytes: Int = 64 * 1024 * 1024,
        admission: ResourceAdmissionController? = nil
    ) throws {
        guard (1...(256 * 1024 * 1024)).contains(maximumBytes) else {
            throw InspectionBudgetError.invalidLimit
        }
        self.maximumBytes = maximumBytes
        self.admission = admission
        account = admission?.makeRetainedByteAccount()
    }

    public var retainedBytes: Int {
        used.withLockedValue { $0 }
    }

    func reserve(_ bytes: Int) -> Bool {
        guard bytes >= 0 else { return false }
        return used.withLockedValue { count in
            guard bytes <= maximumBytes - count else { return false }
            if let account, !account.reserve(bytes) { return false }
            count += bytes
            return true
        }
    }

    func release(_ bytes: Int) {
        used.withLockedValue { count in
            precondition(bytes >= 0 && bytes <= count)
            account?.release(bytes)
            count -= bytes
        }
    }

    deinit {
        used.withLockedValue { count in
            account?.release(count)
            count = 0
        }
    }
}

public enum InspectionBudgetError: Error, Sendable {
    case invalidLimit
    case capacityExceeded
}

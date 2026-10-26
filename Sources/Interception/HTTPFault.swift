public enum HTTPFaultDirection: Sendable { case request, response }
public enum HTTPFaultResetScope: Sendable { case stream, transport }

public struct HTTPFault: Sendable {
    public let direction: HTTPFaultDirection
    public let delayMilliseconds: Int
    public let bytesPerSecond: Int?
    public let burstBytes: Int
    public let resetAfterBytes: UInt64?
    public let resetScope: HTTPFaultResetScope

    public init(
        direction: HTTPFaultDirection,
        delayMilliseconds: Int = 0,
        bytesPerSecond: Int? = nil,
        burstBytes: Int = 65_536,
        resetAfterBytes: UInt64? = nil,
        resetScope: HTTPFaultResetScope = .stream
    ) throws {
        self.direction = direction
        self.delayMilliseconds = delayMilliseconds
        self.bytesPerSecond = bytesPerSecond
        self.burstBytes = burstBytes
        self.resetAfterBytes = resetAfterBytes
        self.resetScope = resetScope
        try validate()
    }

    func validate() throws {
        guard (0...30_000).contains(delayMilliseconds), (1...65_536).contains(burstBytes),
              bytesPerSecond.map({ (1024...1_073_741_824).contains($0) }) ?? true else {
            throw HTTPControlError.invalidConfiguration
        }
    }
}

public struct HTTPFaultAllowance: Sendable {
    public let bytes: Int
    public let waitNanoseconds: UInt64
    public let reset: HTTPFaultResetScope?
}

public struct HTTPFaultSchedule: Sendable {
    public let fault: HTTPFault
    public private(set) var pendingBytes = 0
    public private(set) var forwardedBytes: UInt64 = 0
    private var eligibleNanoseconds: UInt64

    public init(fault: HTTPFault, startNanoseconds: UInt64) {
        self.fault = fault
        let deadline = startNanoseconds.addingReportingOverflow(UInt64(fault.delayMilliseconds) * 1_000_000)
        eligibleNanoseconds = deadline.overflow ? UInt64.max : deadline.partialValue
    }

    public mutating func enqueue(bytes: Int) throws {
        guard bytes >= 0, bytes <= 65_536 - pendingBytes else { throw HTTPControlError.capacityExceeded }
        pendingBytes += bytes
    }

    public func next(nowNanoseconds: UInt64) -> HTTPFaultAllowance {
        if let reset = fault.resetAfterBytes, forwardedBytes >= reset {
            return HTTPFaultAllowance(bytes: 0, waitNanoseconds: 0, reset: fault.resetScope)
        }
        guard nowNanoseconds >= eligibleNanoseconds else {
            return HTTPFaultAllowance(bytes: 0, waitNanoseconds: eligibleNanoseconds - nowNanoseconds, reset: nil)
        }
        var count = min(pendingBytes, fault.burstBytes)
        if let reset = fault.resetAfterBytes {
            count = Int(min(UInt64(count), reset - forwardedBytes))
        }
        return HTTPFaultAllowance(bytes: count, waitNanoseconds: 0, reset: nil)
    }

    public mutating func didForward(bytes: Int, nowNanoseconds: UInt64) throws {
        let allowance = next(nowNanoseconds: nowNanoseconds)
        guard bytes >= 0, bytes <= allowance.bytes, allowance.reset == nil else {
            throw HTTPControlError.invalidConfiguration
        }
        let total = forwardedBytes.addingReportingOverflow(UInt64(bytes))
        guard !total.overflow else { throw HTTPControlError.invalidConfiguration }
        pendingBytes -= bytes
        forwardedBytes = total.partialValue
        if let rate = fault.bytesPerSecond, bytes > 0 {
            let interval = (UInt64(bytes) * 1_000_000_000 + UInt64(rate) - 1) / UInt64(rate)
            let sum = nowNanoseconds.addingReportingOverflow(interval)
            eligibleNanoseconds = sum.overflow ? UInt64.max : sum.partialValue
        }
    }

    public mutating func cancel() { pendingBytes = 0 }
}

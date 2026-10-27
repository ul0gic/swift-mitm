public extension HTTPControlDecision {
    func selectedFault(direction: HTTPFaultDirection) throws -> HTTPFault? {
        let matching = faults.filter { $0.direction == direction }
        guard let first = matching.first else { return nil }
        var delay = first.delayMilliseconds
        var rate = first.bytesPerSecond
        var burst = first.burstBytes
        var reset = first.resetAfterBytes
        var scope = first.resetScope
        for fault in matching.dropFirst() {
            guard fault.delayMilliseconds <= 30_000 - delay else { throw HTTPControlError.invalidConfiguration }
            delay += fault.delayMilliseconds
            burst = min(burst, fault.burstBytes)
            if let candidate = fault.bytesPerSecond {
                rate = rate.map { min($0, candidate) } ?? candidate
            }
            if let candidate = fault.resetAfterBytes {
                if reset == nil || candidate < (reset ?? candidate) {
                    reset = candidate
                    scope = fault.resetScope
                } else if candidate == reset, fault.resetScope == .transport {
                    scope = .transport
                }
            }
        }
        return try HTTPFault(
            direction: direction,
            delayMilliseconds: delay,
            bytesPerSecond: rate,
            burstBytes: burst,
            resetAfterBytes: reset,
            resetScope: scope
        )
    }
}

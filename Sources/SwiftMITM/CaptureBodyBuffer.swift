/// Per-message bounded body capture: copies at most `limit` bytes total, then stops while the
/// caller keeps forwarding the originals downstream. `limit == 0` disables capture (metadata-only).
struct CaptureBodyBuffer {
    private let limit: Int
    private var remaining: Int
    private(set) var truncated = false

    init(limit: Int) {
        self.limit = max(0, limit)
        self.remaining = max(0, limit)
    }

    /// Returns the bounded slice of `chunk` to emit; never copies beyond the remaining budget.
    mutating func take<Bytes: Collection>(_ chunk: Bytes) -> [UInt8] where Bytes.Element == UInt8 {
        let fullSize = chunk.count
        let take = min(remaining, fullSize)
        if limit > 0, take < fullSize { truncated = true }
        guard take > 0 else { return [] }
        remaining -= take
        return Array(chunk.prefix(take))
    }
}

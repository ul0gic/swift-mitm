struct CaptureBodyBuffer {
    private let limit: Int
    private var remaining: Int
    private(set) var truncated = false

    init(limit: Int) {
        self.limit = max(0, limit)
        self.remaining = max(0, limit)
    }

    mutating func take<Bytes: Collection>(_ chunk: Bytes) -> [UInt8] where Bytes.Element == UInt8 {
        let fullSize = chunk.count
        let take = min(remaining, fullSize)
        if limit > 0, take < fullSize {
            truncated = true
        }
        guard take > 0 else { return [] }
        remaining -= take
        return Array(chunk.prefix(take))
    }
}

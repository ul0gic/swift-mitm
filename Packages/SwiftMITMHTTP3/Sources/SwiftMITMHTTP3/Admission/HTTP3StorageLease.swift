import NIOHTTP3

final class HTTP3StorageLease: HTTP3InboundStorageReservation {
    let bytes: Int
    private let kind: HTTP3StorageKind
    private let tracker: HTTP3ResourceTracker

    init(bytes: Int, kind: HTTP3StorageKind, tracker: HTTP3ResourceTracker) {
        self.bytes = bytes
        self.kind = kind
        self.tracker = tracker
    }

    deinit { tracker.release(bytes, kind: kind) }
}

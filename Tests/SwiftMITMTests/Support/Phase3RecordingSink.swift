import NIOConcurrencyHelpers

import SwiftMITM

final class Phase3RecordingSink: CaptureEventSink, @unchecked Sendable {
    private let storage = NIOLockedValueBox<[CaptureEvent]>([])

    var events: [CaptureEvent] { storage.withLockedValue { $0 } }

    func receive(_ event: CaptureEvent) {
        storage.withLockedValue { $0.append(event) }
    }
}

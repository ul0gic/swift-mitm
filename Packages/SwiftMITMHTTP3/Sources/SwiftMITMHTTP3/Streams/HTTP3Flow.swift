import Foundation
import NIOCore
import SwiftMITM

struct HTTP3HeldMessage {
    var message: HTTP3Message
    let lease: HTTP3StorageLease?
}

/// State belongs to the transport event loop, including asynchronous control completions.
final class HTTP3Flow {
    let direction: HTTPFaultDirection
    var source: (any Channel)?
    var destination: (any Channel)?
    var head: HTTP3Message?
    var headLease: HTTP3StorageLease?
    var snapshot: HTTP3HeaderSnapshot?
    var decision: HTTPControlDecision?
    var work: HTTPControlWorkLifetime?
    var task: Task<Void, Never>?
    var waitingForControl = false
    var body: Data?
    var bodyMaximum = 0
    var input: [HTTP3HeldMessage] = []
    var output: [HTTP3HeldMessage] = []
    var pendingBytes = 0
    var writing = false
    var reading = false
    var inReadBatch = false
    var inputEnded = false
    var inputClosed = false
    var sourceInactive = false
    var discardInput = false
    var outputEnded = false
    var capturedBytes = 0
    var effectiveBytes = 0
    var captureTruncated = false
    var effectiveTruncated = false
    var fault: HTTPFaultSchedule?
    var timer: Scheduled<Void>?
    var generatedBody: Data?
    var generatedOffset = 0
    var generatedEnd: HTTP3HeldMessage?

    init(direction: HTTPFaultDirection, source: (any Channel)? = nil, destination: (any Channel)? = nil) {
        self.direction = direction
        self.source = source
        self.destination = destination
    }

    var isRequest: Bool { direction == .request }
    var phase: HTTPControlPhase { isRequest ? .request : .response }
    var observation: HTTPObservationDirection { isRequest ? .request : .response }
    var inputDisposed: Bool { inputClosed || (discardInput && sourceInactive) }

    func disposeQueuedStorage() {
        pendingBytes -= input.reduce(0) { $0 + ($1.message.body?.readableBytes ?? 0) }
        input.removeAll()
        pendingBytes -= output.reduce(0) { $0 + ($1.message.body?.readableBytes ?? 0) }
        output.removeAll()
        generatedBody = nil
        generatedEnd = nil
        body = nil
        timer?.cancel()
        timer = nil
        fault?.cancel()
    }
}

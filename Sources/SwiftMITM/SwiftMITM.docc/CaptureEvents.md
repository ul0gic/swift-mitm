# Capture Events

Consume ordered stream events without blocking network-processing paths or building an unbounded queue.

## Overview

### Event model

``CaptureEvent`` uses request IDs for HTTP correlation and connection IDs for WebSocket correlation.

| Traffic stage | Events |
| --- | --- |
| Request | `requestHead`, zero or more `requestBodyChunk`, optional `requestTrailers`, then `requestEnd` |
| Response | One or more informational `responseHead`, one final `responseHead`, zero or more `responseBodyChunk`, optional `responseTrailers`, then `responseEnd` |
| Stream failure | One `streamError` carrying the affected request ID and a non-payload diagnostic message |
| WebSocket | `webSocketOpen`, zero or more `webSocketFrame`, then one `webSocketClose` |

Ordering is guaranteed within one HTTP stream or WebSocket connection. Independent connections and HTTP/2 streams may call the same ``CaptureEventSink`` concurrently, and no global order is defined.

### Interpret body and frame sizes

For body chunks and ``CapturedWebSocketFrame`` values, `bytes` contains the retained capture slice while `byteCount` reports the full observed wire payload for that event. End events and frames expose truncation state. Forwarding continues with the full payload regardless of the capture limit.

A `captureBodyLimit` of `0` retains no HTTP body bytes. A positive limit applies independently to request and response capture. WebSocket payload capture uses the same configured limit per frame while still reporting the observed frame size.

When `permessageDeflate` or a frame's `compressed` value is true, captured frame bytes remain the unmasked compressed wire payload. SwiftMITM does not inflate or reassemble compressed WebSocket messages.

### Respect synchronous delivery

``CaptureEventSink/receive(_:)`` completes inline before protocol processing continues. Blocking it stalls the owning event loop and may delay unrelated channels assigned to that loop. SwiftMITM does not create an internal queue and does not recursively invoke the sink for one stream.

Perform only constant-time, bounded work in `receive(_:)`. If the application needs asynchronous persistence or analysis, hand events to an explicitly bounded consumer-owned queue with an observable overflow policy.

This adapter uses a bounded `AsyncStream`. The application supplies a constant-time drop metric and owns the consumer task and shutdown sequence:

```swift
import os

final class CaptureMetrics: Sendable {
    private let droppedEvents = OSAllocatedUnfairLock(initialState: 0)

    func recordDrop() {
        droppedEvents.withLock { $0 += 1 }
    }

    var dropCount: Int {
        droppedEvents.withLock { $0 }
    }
}

struct BoundedCaptureSink: CaptureEventSink {
    let continuation: AsyncStream<CaptureEvent>.Continuation
    let recordDrop: @Sendable () -> Void

    func receive(_ event: CaptureEvent) {
        switch continuation.yield(event) {
        case .enqueued:
            break
        case .dropped:
            recordDrop()
        case .terminated:
            break
        @unknown default:
            break
        }
    }
}

let (events, continuation) = AsyncStream.makeStream(
    of: CaptureEvent.self,
    bufferingPolicy: .bufferingOldest(512)
)
let metrics = CaptureMetrics()
let sink = BoundedCaptureSink(
    continuation: continuation,
    recordDrop: metrics.recordDrop
)
```

Choose queue capacity together with `captureBodyLimit`: an event-count bound alone is not a strict byte bound. If retained-byte accounting is required, implement it in the consumer's queue and make rejection observable. Do not log headers, bodies, cookies, authorization values, or WebSocket payloads from the drop path.

Call `continuation.finish()` when capture ends, cancel or await the consumer task according to application ownership, and decide whether shutdown drains or discards queued events. Those policies are intentionally outside the proxy lifecycle.

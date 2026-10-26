# Capture Events

Consume bounded HTTP, WebSocket, opaque-flow, and connection-failure events without blocking network-processing paths.

## Event model

``CaptureEvent`` uses request IDs for HTTP correlation, connection IDs for WebSocket correlation, and flow IDs for opaque TCP correlation.

| Traffic stage | Events |
| --- | --- |
| Request | `requestHead`, zero or more `requestBodyChunk`, optional `requestTrailers`, then `requestEnd` |
| Response | Zero or more informational `responseHead`, one final `responseHead`, zero or more `responseBodyChunk`, optional `responseTrailers`, then `responseEnd` |
| Stream failure | One `streamError` carrying the affected request ID and a non-payload diagnostic message |
| WebSocket | `webSocketOpen`, zero or more `webSocketFrame`, then one `webSocketClose` |
| Opaque TCP | `opaqueOpen`, zero or more `opaqueData` in each direction, one `opaqueDirectionEnd` per cleanly ended direction, then `opaqueClose` |
| Connection failure | One `connectionFailure` when transparent setup fails before a structured or opaque flow takes ownership |

Ordering is guaranteed within one HTTP stream, WebSocket connection, or opaque flow. Independent connections and HTTP/2 streams may call the same ``CaptureEventSink`` concurrently; no global ordering exists.

## Read target metadata

``CapturedRequestHead/target`` is optional. Existing explicit `CONNECT` request construction remains valid without it. When SwiftMITM knows the target, ``CapturedTarget`` separates:

- `destination`: physical remote address and port used for upstream routing
- `logicalAuthority`: authority associated with the connection
- `tlsServerName`: TLS SNI when present
- `ingressProvenance`: explicit `CONNECT` or trusted PROXY v2
- `originalClient`: PROXY v2 source endpoint when trusted transparent ingress supplied it

In transparent mode, the physical destination is authoritative for routing. SNI changes TLS identity selection, not the route.

## Interpret retained bytes and counts

For HTTP body chunks, ``CapturedWebSocketFrame``, and opaque data, `bytes` is the retained capture slice while `byteCount` is the complete observed wire payload size. Forwarding continues with the full payload regardless of a capture limit.

`captureBodyLimit` defaults to `0`: HTTP metadata is emitted without retained request or response body bytes. The same limit applies independently to each HTTP direction and to each WebSocket frame. `opaqueCaptureByteLimit` also defaults to `0`, independently per opaque direction. Opaque data events still carry full observed chunk counts with empty retained bytes at that limit.

Each `opaqueDirectionEnd` reports the direction's complete observed count and whether the configured opaque-retention limit truncated bytes. A clean flow emits exactly one close only after both directions have ended. An opaque transport failure emits exactly one `opaqueError`; no later direction-end or close event follows. Cancellation emits exactly one `opaqueClose` with `.cancelled`.

## Handle transparent failures

``CapturedConnectionFailure`` identifies a failed transparent connection with an ID, timestamp, typed reason, and target metadata when the target was known. Reasons cover peer admission, PROXY metadata and transport, classification, destination and upstream setup, TLS, transport, timeout, and cancellation failures. These events contain no payload bytes or free-form diagnostic text.

After opaque forwarding begins, failures use `opaqueError` for that flow instead of `connectionFailure`. A consumer can therefore treat `connectionFailure` as a pre-flow terminal and `opaqueError` as an opaque-flow terminal.

## WebSocket over HTTP/2

SwiftMITM captures RFC 8441 WebSockets carried by a valid HTTP/2 extended `CONNECT`. It waits for the origin's initial HTTP/2 settings and advertises extended `CONNECT` downstream only when the origin enables it. A client must send `:method = CONNECT`, `:protocol = websocket`, `:scheme`, `:authority`, and `:path`; the origin must return a final 2xx response without ending the stream.

The HTTP request and response heads retain their normal HTTP/2 events. The request head's `id` is also the `connectionID` in ensuing WebSocket events. A rejected handshake remains ordinary HTTP capture and emits no WebSocket lifecycle events. Each extended `CONNECT` stream has independent WebSocket state; a protocol failure, reset, or connection loss on one stream does not change capture for others.

## Respect synchronous delivery

``CaptureEventSink/receive(_:)`` completes inline before protocol processing continues. Blocking it stalls the owning event loop and can delay unrelated channels assigned to that loop. SwiftMITM does not create an internal queue or recursively invoke the sink for one stream.

Perform only constant-time bounded work in `receive(_:)`. If persistence or analysis is asynchronous, hand off to a consumer-owned queue with an explicit event and retained-byte bound, observable overflow, and a defined shutdown policy. Do not log headers, bodies, cookies, authorization values, WebSocket payloads, or opaque payloads on its drop path.

## Migrate exhaustive event handling for 0.2.0

0.2.0 is source-breaking for exhaustive `CaptureEvent` switches. Add handling for `opaqueOpen`, `opaqueData`, `opaqueDirectionEnd`, `opaqueClose`, `opaqueError`, and `connectionFailure`. Update any custom serialization or event schema to preserve the new IDs, typed failure reasons, and target metadata. Consumers that construct ``CapturedRequestHead`` can continue using the initializer without `target`; consumers that match it must handle the optional field.

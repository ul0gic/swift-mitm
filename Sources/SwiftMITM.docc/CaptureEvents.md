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
| Connection failure | `connectionFailure` for setup failures or a connection-level resource limit during HTTP/1 capture |

Ordering is guaranteed within one HTTP stream, WebSocket connection, or opaque flow. Independent connections and HTTP/2 streams may call the same ``CaptureEventSink`` concurrently; no global ordering exists.

## Read target metadata

``CapturedRequestHead/target`` is optional. Existing explicit `CONNECT` request construction remains valid without it. When SwiftMITM knows the target, ``CapturedTarget`` separates:

- `destination` gives the connected peer address and port when available, otherwise the target host and port.
- `logicalAuthority` identifies the authority associated with the connection.
- `tlsServerName` gives the selected TLS name from inspected SNI or the target's DNS name. An IP literal alone supplies no DNS name.
- `ingressProvenance` identifies explicit `CONNECT`, explicit HTTP forwarding, prior-knowledge HTTP/2, or trusted PROXY v2.
- `originalClient` gives the PROXY v2 source endpoint when trusted transparent ingress supplied it.

In transparent mode, the physical destination is authoritative for routing. SNI changes TLS identity selection, not the route.

The explicit HTTP forwarding path uses an in-repository codec fork. It maps permitted received header and trailer octets back to equal octets during serialization. It does not preserve original whitespace or complete wire formatting. The raw interception path continues forwarding original HTTP octets.

## Interpret retained bytes and counts

For HTTP body chunks, ``CapturedWebSocketFrame``, and opaque data, `bytes` is the retained capture slice while `byteCount` is the complete observed wire payload size. Forwarding continues with the full payload regardless of a capture limit.

`captureBodyLimit` defaults to `0`: HTTP metadata is emitted without retained request or response body bytes. The same limit applies independently to each HTTP direction and to each WebSocket frame. `opaqueCaptureByteLimit` also defaults to `0`, independently per opaque direction. Opaque data events still carry full observed chunk counts with empty retained bytes at that limit.

Each `opaqueDirectionEnd` reports the direction's complete observed count and whether the configured opaque-retention limit truncated bytes. A clean flow emits exactly one close only after both directions have ended. An opaque transport failure emits exactly one `opaqueError`; no later direction-end or close event follows. Cancellation emits exactly one `opaqueClose` with `.cancelled`.

## Handle setup failures

``CapturedConnectionFailure`` identifies a failed connection with an ID, timestamp, typed reason, and target metadata when the target was known. Its optional `stage` is one of `admission`, `routing`, `connect`, `upstreamTLS`, `downstreamTLS`, `negotiation`, or `capture`. Its optional `code` is one of `egressDenied`, `setupTimedOut`, `serverStopping`, `invalidProxyMetadata`, `untrustedPeer`, `destinationUnavailable`, `classificationFailed`, `malformedClientHello`, `unsupportedProtocol`, `protocolMismatch`, `upstreamConnectFailed`, `tlsHandshakeFailed`, `transportFailure`, or `resourceLimit`. These events contain no payload bytes or free-form diagnostic text. Platform error strings are not part of the public diagnostic contract.

An `opaqueError` terminates an established opaque flow. An established HTTP/1 connection can emit a `connectionFailure` with stage `.capture` and code `.resourceLimit` when it exceeds an admission limit. Track unfinished HTTP exchanges by their request IDs.

``CapturedOpaqueFlow/reason`` distinguishes `.policySelected` pass-through from `.classificationFallback`. The initializer defaults to `.unspecified` for consumer-constructed values. Pass-through forwards application bytes unchanged. TLS remains end-to-end, while a plaintext protocol can expose plaintext in `opaqueData` when opaque payload retention is enabled.

## Correlate HTTP/1 exchanges

`HTTP1CorrelationPolicy` bounds pending exchange count and retained method bytes per connection. Defaults are 128 exchanges and 65,536 method bytes; the accepted ranges are 1–1,024 and 1–1,048,576 respectively. Exceeding either limit closes the affected connection and fails outstanding unfinished captures. It does not produce a successful terminal event for an exchange that did not complete.

## Interpret streaming protocols

Raw `CaptureEvent` values describe incremental HTTP body chunks and trailers; they do not carry decoded SSE, protobuf or gRPC messages. The optional `SwiftMITMInspection` product provides those semantic observations, including gRPC-Web and supported content decoding, through a separate `InspectionEventSink`. Capture retention limits do not truncate the full bytes supplied at the observation boundary. Consumer receive timestamps are arrival observations, not packet or kernel timestamps.

## WebSocket over HTTP/2

SwiftMITM captures RFC 8441 WebSockets carried by a valid HTTP/2 extended `CONNECT`. It waits for the origin's initial HTTP/2 settings and advertises extended `CONNECT` downstream only when the origin enables it. A client must send `:method = CONNECT`, `:protocol = websocket`, `:scheme`, `:authority`, and `:path`; the origin must return a final 2xx response without ending the stream.

The HTTP request and response heads retain their normal HTTP/2 events. The request head's `id` is also the `connectionID` in ensuing WebSocket events. A rejected handshake remains ordinary HTTP capture and emits no WebSocket lifecycle events. Each extended `CONNECT` stream has independent WebSocket state; a protocol failure, reset, or connection loss on one stream does not change capture for others.

## Respect synchronous delivery

``CaptureEventSink/receive(_:)`` completes inline before protocol processing continues. Blocking it stalls the owning event loop and can delay unrelated channels assigned to that loop. SwiftMITM does not create an internal queue or recursively invoke the sink for one stream.

Perform only constant-time bounded work in `receive(_:)`. If persistence or analysis is asynchronous, hand off to a consumer-owned queue with an explicit event and retained-byte bound, observable overflow, and a defined shutdown policy. Do not log headers, bodies, cookies, authorization values, WebSocket payloads, or opaque payloads on its drop path.

## Migrate exhaustive event handling for 0.2.0

0.2.0 is source-breaking for exhaustive `CaptureEvent` switches. Add handling for `opaqueOpen`, `opaqueData`, `opaqueDirectionEnd`, `opaqueClose`, `opaqueError`, and `connectionFailure`. Update any custom serialization or event schema to preserve the new IDs, typed failure reasons, and target metadata. Consumers that construct ``CapturedRequestHead`` can continue using the initializer without `target`; consumers that match it must handle the optional field.

## Migrate from 0.2.1 to 0.2.2 or later

Version 0.2.2 adds `.explicitHTTP` and `.priorKnowledgeHTTP2` to both ``ProxyIngress`` and ``CapturedIngressProvenance``. Handle both new cases in exhaustive switches. The explicit HTTP ingress accepts HTTP/1.1 absolute-form `http` requests and HTTPS `CONNECT`. The prior-knowledge ingress uses a configured HTTP/2 destination. The default is `.explicitConnect`.

`ProxyServer` adds defaulted `correlationPolicy` and `interceptionPolicy` initializer parameters. `CapturedOpaqueFlow` adds a defaulted `reason`. `CapturedConnectionFailure` adds defaulted optional `stage` and `code` values. Existing value construction remains source-compatible, but serialized event schemas should add the new fields if they preserve these diagnostics.

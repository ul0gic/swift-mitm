# Inspect HTTP streams

Use `InterceptionLab` when you need a console observer for HTTP traffic routed through an explicit proxy. It wires `HTTPInspector` to `ProxyServer`'s public `HTTPObservationSink` boundary and prints `InspectionEvent` IDs, direction, representation, sequence and payload sizes. Request method and authority are also printed, truncated to 256 characters each. Header values, body contents, protobuf field values and WebSocket message contents are not printed.

## Start the observer

For generated certificates and repository-owned peers, follow [Run the local interception lab](LocalLab.md). The commands below are the equivalent configuration for an origin you already operate.

Start an HTTPS origin you control at `localhost:9443` that serves `text/event-stream` at `/events`. The commands assume your P-256 interception CA certificate and matching key are in `$HOME/.swiftmitm/`, alongside the origin's CA certificate, `origin-ca.pem`. Start the proxy from `Examples/InterceptionLab`.

```sh
swift run InterceptionLab serve "$HOME/.swiftmitm/ca-cert.pem" "$HOME/.swiftmitm/ca-key.pem" \
  --allow-internal --origin-ca "$HOME/.swiftmitm/origin-ca.pem"
```

The proxy listens on loopback port 8080. Stop it with Control-C or SIGTERM. Control-C drains queued output while stdout remains immediately writable; SIGTERM cancels delivery before stopping the proxy. On a normal drain, an observation loss or incomplete inspector drain prints `inspectionComplete=false` and suppresses the normal `stopped` record. Output queue loss is reported separately as `deliveryLoss` and marks active console captures incomplete. The command changes neither system proxy settings nor system trust.

Console delivery holds at most 256 records and 1 MiB of queued UTF-8, plus one record in flight. If stdout cannot keep up, the example drops console delivery, reports loss on stderr when writable, and keeps proxy forwarding until shutdown. The queue is not a process-memory bound.

The sample enables internal egress for this proxy process. The proxy adds the origin CA to its system trust roots. In a second terminal, give curl the interception CA and request the stream.

```sh
curl --noproxy '' --no-buffer --proxy http://127.0.0.1:8080 --cacert "$HOME/.swiftmitm/ca-cert.pem" https://localhost:9443/events
```

The inspector decodes incoming bytes incrementally under a separate budget. The console supplies no protobuf descriptor set, so schema-dependent decoding emits a diagnostic. An embedding application can read its typed cause and supply a schema through `InspectionConfiguration`.

The `original` representation records traffic before control edits. On the controlled path, `effective` observations describe the forwarded or synthesized exchange, including requests that pass through unchanged. The console labels each representation.

For SSE events, `decodedRange` ends just after the CR or LF that dispatches the record. If that CR is followed by LF, the next record starts after the LF. For uncompressed bodies, `wireRange` equals `decodedRange`. For compressed bodies, it covers the input prefix consumed by the outer decoder when the event was emitted.

## Exercise the semantic paths

Use controlled clients and origins that you operate. Configure each client for the explicit proxy and make it trust `ca-cert.pem`:

| Workflow | Controlled peer | What the console prints |
|---|---|---|
| SSE | An HTTPS endpoint that flushes `text/event-stream` records before the response ends. | Incremental `sse` events and sequence numbers. |
| gRPC or gRPC-Web | An RPC client and TLS service configured for the explicit proxy. | `grpc event` for gRPC observations and `diagnostic` for inspection errors. |
| WebSocket | A controlled `wss` endpoint and a client that sends a fragmented message through CONNECT. | `webSocket event` for frame and lifecycle events, plus `webSocketMessage decodedBytes=` for a reassembled message. |

The executable does not bundle or start protocol peers. A peer's successful response does not prove a semantic event; inspect the console transcript. Unsupported coding, malformed input, missing schema and configured limits produce diagnostics while passive forwarding remains the transport behavior. See [Active debugging](ActiveDebugging.md) for the opt-in control path.

## Feature and compatibility limits

| Capability | Public path | Limit |
|---|---|---|
| HTTP/1.1 and HTTP/2 | Explicit HTTP, HTTPS CONNECT, and configured prior-knowledge HTTP/2 | HTTP/3 and QUIC are unsupported. |
| HTTP capture and observation | `ProxyServer` plus `HTTPInspector` | The inspector is opt-in; raw capture retention remains separate. |
| Content decoding | `gzip`, `deflate`, and Brotli content codings | Malformed or unsupported coding ends that observation with a diagnostic. |
| SSE | Incremental event-stream parser | Event and line limits are configurable through `InspectionConfiguration`. |
| gRPC | Native gRPC, binary gRPC-Web, and text gRPC-Web framing | Protobuf field interpretation requires a supplied descriptor schema. |
| WebSocket messages | Message decoder for supported permessage-deflate negotiation | Frame events and message observations are distinct; unsupported negotiation is reported. |

The console uses these `InspectionConfiguration` defaults: 64 MiB shared retained-storage budget; 4 MiB and 1,024 items queued; 256 active bodies; 64 KiB SSE lines; 1 MiB SSE events; and 4 MiB gRPC/WebSocket messages. The configuration permits larger values up to 256 MiB budget, 256 MiB queue, 65,536 queued items, 1,024 active bodies, 256 KiB lines, 4 MiB events and 16 MiB messages. These are library limits, not measured process-RSS bounds; native decoder workspace is outside the application-owned budget.

The core and inspection libraries target macOS 26 and Swift 6.4 with Crypto 5.

For application integration, descriptor loading and shutdown ordering, use [Embed semantic inspection](Embedding.md).

## Share admission accounting

`InspectionMemoryBudget(admission:)` can charge inspector-owned buffers to the same `ResourceAdmissionController` passed to `ProxyServer`. The local inspection limit and shared retained-byte limit both apply; either can stop observation without changing forwarded traffic. `InterceptionLab` supplies the same controller to both owners.

Passing a controller only to `ProxyServer` does not adopt an independently constructed inspection budget. Supply that instance explicitly to every inspection budget that belongs to the shared quota. Host-retained sink copies and opaque native codec workspace are outside these application-byte counters; they are not a process RSS limit.

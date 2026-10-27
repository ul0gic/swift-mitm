# Inspect HTTP streams

Use `InterceptionLab` when you need a console observer for HTTP traffic routed through an explicit proxy. It wires `HTTPInspector` to `ProxyServer`'s public `HTTPObservationSink` boundary and prints `InspectionEvent` IDs, direction, representation, sequence and payload sizes. Request method and authority are also printed, truncated to 256 characters each. Header values, body contents, protobuf field values and WebSocket message contents are not printed.

## Start the observer

For generated certificates and repository-owned peers, follow [Run the local interception lab](LocalLab.md). The commands below are the equivalent configuration for an origin you already operate.

Use an existing matching P-256 interception CA certificate and private key. Keep the private key outside the repository. The client trusts this CA; upstream certificate verification remains enabled. From `Examples/InterceptionLab`:

```sh
swift run InterceptionLab serve ca-cert.pem ca-key.pem
```

The proxy listens on loopback port 8080. Stop it with Control-C or SIGTERM. Control-C drains queued output while stdout remains immediately writable; SIGTERM cancels delivery before stopping the proxy. On a normal drain, an observation loss or incomplete inspector drain prints `inspectionComplete=false` and suppresses the normal `stopped` record. Output queue loss is reported separately as `deliveryLoss` and marks active console captures incomplete. The command changes neither system proxy settings nor system trust.

Console delivery holds at most 256 records and 1 MiB of queued UTF-8, plus one record in flight. If stdout cannot keep up, the example drops console delivery, reports loss on stderr when writable, and keeps proxy forwarding until shutdown. The queue is not a process-memory bound.

Send a request through the proxy:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 --cacert ca-cert.pem https://localhost:9443/events
```

This command expects an HTTPS origin you control at `localhost:9443` with an `/events` endpoint. For an internal origin, start the proxy with explicit internal egress and its trust root:

```sh
swift run InterceptionLab serve ca-cert.pem ca-key.pem --allow-internal --origin-ca origin-ca.pem
```

`--allow-internal` permits internal destinations generally for this proxy process; it is not restricted to `localhost`. `--origin-ca` supplements system roots and does not disable upstream verification. The client separately trusts the interception CA with `--cacert`. These options do not install trust or change system routing.

The inspector handles selected semantics incrementally as bytes arrive. Semantic observations use a separate bounded budget and do not reconstruct a full message from retained raw-body prefixes. The console configuration does not load a protobuf descriptor set, so schema-dependent decoding reports that the schema is unavailable until an embedding application supplies a schema through `InspectionConfiguration`.

The `original` representation is observed before control edits. An `effective` representation is emitted separately for edited or synthesized traffic; it does not replace original capture evidence. The console prints the representation label and does not merge the two.

## Exercise the semantic paths

Use controlled clients and origins that you operate. Configure each client for the explicit proxy and make it trust `ca-cert.pem`:

| Workflow | Controlled peer | What the console prints |
|---|---|---|
| SSE | An HTTPS endpoint that flushes `text/event-stream` records before the response ends. | Incremental `sse` events and sequence numbers. |
| gRPC or gRPC-Web | An independent RPC client and service configured for the explicit proxy. | Message, status or diagnostic summaries; payloads are not printed. |
| WebSocket | A controlled `ws` or `wss` endpoint and client that sends a fragmented message. | Frame capture and reassembled message summaries when supported negotiation is present. |

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

The 0.2.2 candidate and these commands remain unverified until the candidate's GitHub Actions consumer and protocol checks pass.

## Share admission accounting

`InspectionMemoryBudget(admission:)` can charge inspector-owned buffers to the same `ResourceAdmissionController` passed to `ProxyServer`. The local inspection limit and shared retained-byte limit both apply; either can stop observation without changing forwarded traffic. `InterceptionLab` supplies the same controller to both owners.

Passing a controller only to `ProxyServer` does not adopt an independently constructed inspection budget. Supply that instance explicitly to every inspection budget that belongs to the shared quota. Host-retained sink copies and opaque native codec workspace are outside these application-byte counters; they are not a process RSS limit.

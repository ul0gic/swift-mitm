# FirstCapture

An external SwiftPM executable demonstrating public SwiftMITM HTTP/HTTPS capture, streaming metadata, deliberate TLS pass-through, and signal-driven shutdown. Requires macOS 26 or later and Xcode 27.1 with Swift 6.4. Runtime and commands below remain unverified until the candidate's GitHub Actions consumer checks pass.

## Start

Supply an existing matching P-256 CA private key and CA certificate in PEM format. Keep the private key outside the repository. From this directory, with those files named `ca-cert.pem` and `ca-key.pem`:

```sh
swift run FirstCapture --ca-cert ca-cert.pem --ca-key ca-key.pem --port 8080
```

The listener binds only to `127.0.0.1`. Standard output is JSON Lines, beginning with a record such as `{"type":"listening","port":8080}`; key order is unspecified. `--port 0` chooses an available port and reports it. SIGINT (Control-C) stops the server and drains delivery, emitting `{"type":"stopped"}` only when delivery remains intact. In async mode, draining never waits for stdout readiness: a full pipe cancels delivery. SIGTERM explicitly cancels async delivery before stopping the server. Synchronous mode retains its blocking delivery and graceful drain for both signals.

The example changes neither system proxy settings nor system trust. Standard error contains usage or startup/cleanup failures, without certificate material or private-key contents. Invalid arguments exit 2; other setup/shutdown failures exit 1.

## Capture HTTP and HTTPS

In a second terminal in this directory:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 http://example.com/
curl --noproxy '' --proxy http://127.0.0.1:8080 --cacert ca-cert.pem https://example.com/
```

These commands depend on external network availability. `--cacert` scopes client trust to that invocation; upstream certificate verification remains enabled against system trust. Expect `requestHead`, `responseHead`, body-chunk metadata, and `requestEnd`/`responseEnd` records sharing an `id`. A `streamError` terminates an unfinished capture; `connectionFailure` includes stable reason and available stage/code metadata. CONNECT acceptance alone does not establish upstream TLS success.

Headers, paths, free-form error text, and WebSocket close text are omitted. Method and authority output are limited to 256 characters each. Body records report observed `byteCount` and retained `retainedBytes`; payload retention defaults to zero. To opt into base64 payload output, restart with `--capture-bytes 65536`. This applies the library's body and opaque retention limits; it does not decode SSE, gRPC, compression, or WebSocket content.

## Stream and bypass

For SSE, use your own controlled HTTPS origin with an `/events` endpoint that flushes events while keeping its response open:

```sh
curl --noproxy '' --no-buffer --proxy http://127.0.0.1:8080 --cacert ca-cert.pem https://localhost:9443/events
```

For that local origin, restart FirstCapture with `--allow-internal --upstream-ca origin-ca.pem` in addition to its required CA flags. `origin-ca.pem` must trust the origin's real certificate; this supplements system roots without disabling verification. `--allow-internal` explicitly permits internal destinations and is unnecessary for public origins. The client trusts the interception CA; FirstCapture separately trusts the origin CA. Streaming body-chunk records can arrive before `responseEnd`; byte counts continue after retained payload is exhausted.

For a client that must keep origin-to-client TLS intact, restart with `--passthrough example.com:443` and run:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 https://example.com/
```

The client validates the origin directly using its own trust roots. Expect `opaqueOpen` with `reason:"policySelected"`, opaque byte metadata, and an opaque terminal outcome; decrypted HTTP capture is unavailable. Pass-through uses exact host-and-port rules, can be repeated, and does not bypass egress policy. It is never an automatic retry after failed interception. Normalize international DNS names to ASCII A-labels before supplying rules.

## Bounded async handoff

Add `--async` to move JSON delivery to one consumer task. `BoundedDelivery.swift` is an example-owned single-consumer ring, not a SwiftMITM API. Producers project events into metadata/payload JSON before enqueueing; they never retain the original event, header collection, target object, or error string in the queue. The queue holds at most 256 records and 1,048,576 encoded bytes, including all retained metadata and base64 payload. Fixed ring/object overhead and the consumer's one in-flight record (at most another 1,048,576 encoded bytes plus its newline) are additional; this is not a process-memory bound. Oversized payloads fail before projection; bounded projection can still allocate a temporary encoded record before admission.

Overflow atomically stores `DeliveryError.overflow` outside the queue, discards queued data, and cancels any pending stdout write. The consumer throws and releases its in-flight record. A terminal record such as `{"type":"deliveryLoss","reason":"overflow","allActiveCapturesIncomplete":true}` is attempted on nonblocking stderr independently of stdout progress; stdout receives the record only if no partially written record prevents it and space is immediately available. Stalled or closed output can prevent delivery of the notification itself; the typed failure remains outside the queue. Treat every active capture as incomplete, and do not interpret previous successful individual outcomes as proof of an intact overall history. Subsequent producer events are discarded while proxy forwarding continues until a signal stops it. No successful capture terminals are synthesized. Delivery loss also suppresses the normal `stopped` record.

SIGINT stops the proxy first, then finishes and drains the queue only while stdout is immediately writable. If a write would block, delivery terminates with `DeliveryError.cancelled` and remaining storage is released. SIGTERM explicitly cancels delivery before stopping the proxy. Cancelling the consumer task also wakes a pending read or stdout write; no shutdown path waits for a stalled stdout pipe in async mode. `AsyncStdout.swift` uses nonblocking Darwin writes and a readiness source with at most one in-flight record. Per-descriptor `F_SETNOSIGPIPE` keeps closed stdout/stderr pipes from terminating the process during async delivery; a failed stdout write terminates delivery with `outputFailed` while forwarding continues until a signal. Both descriptors' original nonblocking and SIGPIPE settings are restored after delivery ends or configuration fails. Synchronous callbacks retain stdout backpressure without `--async`. The recipe creates one consumer task, never one task per event; it does not promise lossless capture when output cannot keep up.

## Options

| Option | Default | Meaning |
|---|---|---|
| `--ca-cert PATH`, `--ca-key PATH` | Required | Restore the host-owned interception CA. |
| `--port INT` | `8080` | Loopback port, including `0` for automatic selection. |
| `--capture-bytes INT` | `0` | Nonnegative retained payload limit. |
| `--passthrough AUTHORITY` | None | Repeatable exact authority bypass. |
| `--upstream-ca PATH` | None | Repeatable additional origin trust roots in PEM files. |
| `--async`, `--allow-internal` | Off | Async delivery; explicitly permit internal origins. |

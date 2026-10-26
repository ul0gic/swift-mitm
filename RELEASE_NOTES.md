# SwiftMITM 0.2.2

**Candidate preparation only.** Version 0.2.2 has not passed its release acceptance gate or been published. Version 0.2.1 remains the latest published release.

## Candidate changes

- Opt-in `.explicitHTTP` accepts ordinary HTTP/1.1 absolute-form requests and HTTPS `CONNECT` on one listener. `.explicitConnect` remains the default.
- Exact-authority interception policy can select opaque pass-through. Policy-selected forwarding remains encrypted end-to-end and is distinguished from protocol-classification fallback in capture metadata.
- Capture failures expose optional typed stage and code metadata. HTTP/1 request correlation has configurable per-connection bounds.
- The external `Examples/FirstCapture/` package demonstrates the public proxy integration and has optional bounded asynchronous event delivery. Delivery overflow is terminal and marks active captures incomplete while forwarding continues.
- Candidate SSE and gRPC qualification concerns raw streaming body bytes and HTTP trailers; it does not decode SSE, protobuf, gRPC message boundaries, or compressed payloads.
- The public-import example has optional bounded asynchronous delivery. On SIGINT, `--async` drains immediately writable output and cancels if it would block; on SIGTERM it cancels delivery. Loss marks active captures incomplete and suppresses `stopped`.

## Migration

Exhaustive switches must handle `.explicitHTTP` in `ProxyIngress` and `CapturedIngressProvenance`. Existing explicit CONNECT construction remains unchanged. `ProxyServer` adds defaulted `correlationPolicy` and `interceptionPolicy` parameters. `CapturedOpaqueFlow` adds defaulted `reason`; `CapturedConnectionFailure` adds defaulted optional `stage` and `code`.

Read the [candidate migration guide](Sources/SwiftMITM.docc/CaptureEvents.md#migrate-to-the-0-2-2-candidate) and [Getting Started](Sources/SwiftMITM.docc/GettingStarted.md) before using candidate source.

## Candidate limitations and verification

- Package and example require Swift tools 6.4 and macOS 26+. The build baseline is Xcode 27.1 with bundled Swift 6.4; candidate verification remains pending.
- Explicit HTTP forwarding uses an in-repository codec fork. It maps permitted received header and trailer octets back to equal octets when serializing, but does not preserve original whitespace or full wire formatting. Compilation, linkage, and runtime fidelity remain unverified; raw interception continues forwarding original HTTP octets.
- The configured matrix covers curl, URLSession, WKWebView, Chromium, independent gRPC, and the external example; its candidate results have not completed. The candidate DocC archive is configured as a downloadable workflow artifact; it is not hosted or published yet.

The minimum supported macOS version increases to 26 in 0.2.2; earlier macOS versions are no longer supported.

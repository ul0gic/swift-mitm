# SwiftMITM

**Build network tools that can see inside the connection.**

SwiftMITM is a Swift-native interception engine for macOS developer tools and authorized security workflows. Embed HTTP/HTTPS proxying, inspect HTTP/1.1 and HTTP/2 exchanges, observe WebSocket traffic, and capture streaming bodies through one structured event API.

Build a network inspector, a debugging companion, or a traffic-analysis tool. SwiftMITM handles proxy transport and capture; your application owns the interface, trust, storage, and analysis.

[Run the example](#see-your-first-capture) · [Integration guide](Sources/SwiftMITM.docc/GettingStarted.md) · [Capture API](Sources/SwiftMITM.docc/CaptureEvents.md) · [Security model](Sources/SwiftMITM.docc/SecurityModel.md)

> **0.2.2 candidate:** this README describes the current source candidate. Implementation is prepared; compilation and runtime acceptance remain unverified. The latest published release is [0.2.1](https://github.com/ul0gic/swift-mitm/tree/0.2.1), whose requirements and capabilities differ.

## What you can build

- **Developer network inspectors:** correlate requests and responses, inspect headers and trailers, and retain only the payload bytes you need.
- **Streaming diagnostics:** observe SSE and gRPC transport as body chunks arrive, without waiting for the response to finish.
- **WebSocket tools:** capture RFC 6455 traffic and RFC 8441 extended CONNECT streams over HTTP/2.
- **Security workbenches:** retain upstream certificate verification, constrain destinations, and distinguish routing, TLS, and capture failures.
- **Host-integrated capture:** accept original-destination metadata from a trusted PROXY v2 forwarder.

## See your first capture

[FirstCapture](Examples/FirstCapture/README.md) is a runnable executable built entirely against the public package API. It keeps the proxy alive, writes JSON Lines, and shuts down on Control-C.

Use the candidate checkout, macOS 26+, and Xcode 27.1 with Swift 6.4. Xcode itself requires macOS 26.6+. Supply a matching P-256 interception CA certificate and private key in PEM format, stored outside the repository. The example neither installs trust nor changes system proxy settings.

From the repository root, with those files in `$HOME/.swiftmitm/`:

```sh
cd Examples/FirstCapture
swift run FirstCapture \
  --ca-cert "$HOME/.swiftmitm/ca-cert.pem" \
  --ca-key "$HOME/.swiftmitm/ca-key.pem" \
  --port 8080
```

The first record reports the listening port:

```json
{"type":"listening","port":8080}
```

In another terminal, send a request through the proxy:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 http://example.com/
```

For HTTPS, trust the interception CA for that invocation:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 \
  --cacert "$HOME/.swiftmitm/ca-cert.pem" https://example.com/
```

Look for `requestHead`, `responseHead`, body-chunk metadata, and `responseEnd` records correlated by `id`. Payload retention defaults to zero. Requests depend on external network availability; these candidate commands have not yet passed the GitHub acceptance run.

The [example guide](Examples/FirstCapture/README.md) covers payload capture, SSE, intentional pass-through, custom origin trust, and bounded asynchronous output. The [CA guide](Sources/SwiftMITM.docc/CertificateAuthorityAndTrust.md) covers creation, persistence, and client trust.

## Built around traffic integrity

| Capability | What it gives your tool |
| --- | --- |
| HTTP and HTTPS on one listener | Opt-in `.explicitHTTP` handles ordinary HTTP requests and HTTPS CONNECT; CONNECT-only remains the default. |
| Header and trailer fidelity | Explicit HTTP codecs preserve permitted field-value octets, including non-UTF-8 bytes; raw interception forwards original HTTP octets. |
| Streaming observation | Incremental body capture and trailers for SSE/gRPC transport, with retention limits independent of forwarding. |
| Deliberate encrypted pass-through | Exact host-and-port rules preserve end-to-end TLS and report why traffic is opaque. |
| Bounded capture state | Per-direction retention limits, bounded request correlation, and SwiftNIO backpressure. |
| Explicit failure outcomes | Structured connection stages/codes and capture terminal events expose incomplete exchanges. |

Header fidelity does not mean identical wire formatting: the explicit forward proxy can normalize whitespace and framing. Its codec is a narrowly patched, pinned SwiftNIO source fork with a separately namespaced llhttp parser; consumers retain the official public NIO types.

Pass-through is selected before interception. A failed handshake or certificate check never silently turns into an encrypted bypass.

## Embed it in your application

The library product is `SwiftMITM`; both the library and example require Swift tools 6.4 and target macOS 26+. Candidate CI selects Xcode 27.1 and separately checks the standalone Swift 6.4.1 compiler.

For candidate development, add this checkout as a local Swift package dependency and link the `SwiftMITM` product. The runnable [example manifest](Examples/FirstCapture/Package.swift) shows the exact dependency declaration. There is no published 0.2.2 tag yet; use the [0.2.1 documentation](https://github.com/ul0gic/swift-mitm/tree/0.2.1) when integrating the published release.

The integration has three parts:

1. Provide your interception `CertificateAuthority` and a `CaptureEventSink`.
2. Choose explicit HTTP/CONNECT or trusted PROXY v2 ingress and start `ProxyServer`.
3. Consume events throughout the session, then await `stop()` during shutdown.

`CaptureEventSink.receive(_:)` is synchronous and may be called concurrently by independent connections. Slow callbacks apply backpressure. If you move delivery off the event loop, bound the queue and make loss visible; [FirstCapture's async mode](Examples/FirstCapture/README.md#bounded-async-handoff) demonstrates that contract.

Start with [Getting Started](Sources/SwiftMITM.docc/GettingStarted.md), then use [Capture Events](Sources/SwiftMITM.docc/CaptureEvents.md) and [Lifecycle and Concurrency](Sources/SwiftMITM.docc/LifecycleAndConcurrency.md). Upgrading consumers should read the [0.2.2 migration notes](Sources/SwiftMITM.docc/CaptureEvents.md#migrate-to-the-0-2-2-candidate).

## Security defaults that stay visible

- The listener binds to loopback; exposing it requires explicit configuration and host-managed access control.
- Upstream TLS certificate verification is enabled; additional trust roots augment the system roots.
- Internal, loopback, link-local, and unspecified upstream destinations are denied unless explicitly permitted.
- Body and opaque payload retention default to zero; headers and other traffic metadata are still sensitive.
- Transparent ingress admits the actual TCP peer before trusting its PROXY v2 metadata.

Your application owns authorization, CA-key persistence, client trust, capture storage, redaction, and any system routing changes. SwiftMITM does not install certificates, configure system proxies, or manage privileged helpers. Read the [security model](Sources/SwiftMITM.docc/SecurityModel.md) before exposing a listener or retaining traffic.

For host-forwarder integration, use the versioned [PROXY v2 conformance contract](Conformance/ProxyV2/README.md). A host helper supplies the original destination; the package does not implement direct `/dev/pf` destination lookup.

## Know the boundaries

SSE and gRPC support is **transport capture**: raw streamed body bytes and HTTP trailers. SwiftMITM does not decode SSE events, protobuf messages, gRPC message boundaries, compression, or gRPC-Web.

There is no UDP, QUIC, HTTP/3, cleartext HTTP/2 prior knowledge, or `h2c` upgrade. There is no ECH decryption, general opaque-protocol decoding, or HTTP/2-to-HTTP/1.1 WebSocket translation. Certificate pinning can prevent interception; intentional pass-through preserves connectivity without providing decrypted capture.

## Release qualification

GitHub Actions owns compilation, lint, security analysis, DocC, external-consumer builds, and behavioral qualification. The prepared matrix covers curl, URLSession, WKWebView, Chromium, independent gRPC peers, and large-transfer/resource scenarios.

All 0.2.2 candidate results remain unverified. Full suites are configured on macOS 27; the macOS 26 public-consumer smoke is partial minimum-runtime evidence, not full client qualification. No performance claim or production-readiness claim is implied by the prepared checks.

[Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md) · [Support](SUPPORT.md) · [Report a vulnerability](SECURITY.md)

## License

SwiftMITM is [MIT licensed](LICENSE). Vendored SwiftNIO codec sources retain their Apache 2.0 license and notices; llhttp retains its MIT license and the SwiftNIO wrapper's Apache 2.0 license and notice. See [Vendor/](Vendor/) for the retained licenses and source provenance.

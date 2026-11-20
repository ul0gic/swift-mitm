# SwiftMITM

**Inspect, intercept, and replay network traffic from Swift.**

SwiftMITM is an embeddable MITM engine for macOS developer tools and authorized security workflows. Build network inspectors, API debuggers, and traffic-analysis tools on Apple's SwiftNIO transport stack. Your application owns the interface, client trust, and storage.

[Documentation](Documentation/README.md) · [Quick start](#quick-start) · [Local lab](Documentation/LocalLab.md) · [Integration guide](Sources/SwiftMITM.docc/GettingStarted.md) · [Security model](Sources/SwiftMITM.docc/SecurityModel.md)

## Features

- HTTP/1.1 and HTTP/2 interception, ordinary HTTP proxying, HTTPS CONNECT, configured prior-knowledge HTTP/2, and trusted PROXY v2 ingress.
- Streaming SSE, native gRPC, binary/text gRPC-Web, descriptor-based protobuf inspection, and gzip, deflate, and Brotli decoding.
- WebSocket frame capture and fragmented-message inspection, including supported permessage-deflate negotiation and RFC 8441 transport.
- Ordered HTTP rules, header edits, bounded body replacement, synthetic responses, destination mapping, breakpoints, delays, throttling, and resets.
- Explicit request replay and streaming HAR 1.2 export with credential redaction, measured timings, and original/effective capture provenance.
- Configurable capture and inspection budgets, shared resource admission, scoped HTTP/1 connection reuse, and deliberate encrypted pass-through.

For intercepted TLS over TCP, SwiftMITM buffers a fragmented ClientHello, selects HTTP/2 or HTTP/1.1 with the origin first, then replays the buffered bytes into the downstream TLS handler.

Inspection happens before raw payload-retention limits. Original traffic and edited traffic remain separate representations. A narrow SwiftNIO codec patch preserves header-value octets. Explicit forwarding can still normalize whitespace and framing.

## Quick start

SwiftMITM requires macOS 26 or later and Swift tools 6.4. Pull requests build with Xcode 27.1. [FirstCapture](Examples/FirstCapture/README.md) runs a proxy and prints JSON Lines through the public API.

Supply a matching P-256 interception CA certificate and private key in `$HOME/.swiftmitm/`, outside the repository. See the [CA guide](Sources/SwiftMITM.docc/CertificateAuthorityAndTrust.md), or use the [local lab](Documentation/LocalLab.md) for generated credentials and controlled peers.

From the repository root:

```sh
cd Examples/FirstCapture
swift run FirstCapture \
  --ca-cert "$HOME/.swiftmitm/ca-cert.pem" \
  --ca-key "$HOME/.swiftmitm/ca-key.pem" \
  --port 8080
```

After the `listening` record, send traffic from another terminal:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 \
  --cacert "$HOME/.swiftmitm/ca-cert.pem" https://example.com/
```

Look for correlated `requestHead`, `responseHead`, body-chunk metadata, and `responseEnd` records. Payload retention defaults to zero. Control-C stops the proxy; neither example changes system trust or proxy settings.

For a runnable HTTPS/SSE observer, paused header edit, replay, and redacted HAR demonstration, run the [local InterceptionLab](Documentation/LocalLab.md).

## Embed it

Link `SwiftMITM` for transport, capture, controls, replay, and export. Add `SwiftMITMInspection` for semantic inspection. The [example manifest](Examples/InterceptionLab/Package.swift) shows both products.

Create a `CertificateAuthority`, supply a `CaptureEventSink`, configure `ProxyServer`, and consume events until shutdown. Capture callbacks are synchronous and can arrive concurrently; use a bounded handoff for slow consumers.

[Embedding inspection](Documentation/Embedding.md) · [Capture API](Sources/SwiftMITM.docc/CaptureEvents.md) · [Inspection](Documentation/Inspection.md) · [Active debugging](Documentation/ActiveDebugging.md) · [Lifecycle](Sources/SwiftMITM.docc/LifecycleAndConcurrency.md)

## Security and boundaries

Loopback binding, upstream certificate verification, and restricted internal destinations are defaults. Your host application controls authorization, CA-key persistence, client trust, and retained traffic. See the [security model](Sources/SwiftMITM.docc/SecurityModel.md).

HTTP/3, QUIC, `h2c` upgrade, and ECH decryption are unsupported. HTTP controls do not edit CONNECT or upgrade handshakes. Certificate pinning can prevent interception; explicit pass-through preserves encrypted connectivity without decrypted capture.

## License

SwiftMITM is [MIT licensed](LICENSE). Vendored SwiftNIO codec sources retain Apache 2.0 notices; llhttp retains its MIT license and the SwiftNIO wrapper's Apache 2.0 notices. See [Vendor/](Vendor/).

[Contributing](CONTRIBUTING.md) · [Release notes](RELEASE_NOTES.md) · [Changelog](CHANGELOG.md) · [Support](SUPPORT.md) · [Report a vulnerability](SECURITY.md)

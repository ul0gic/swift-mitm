# Changelog

All notable SwiftMITM changes are recorded here. The project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/).

## Unreleased

## 0.2.0 - 2026-08-20

### Added

- HTTP/2 WebSocket capture through origin-capability-gated RFC 8441 extended CONNECT, with existing event correlation, bounded frame retention, and isolated stream failures.
- Trusted PROXY protocol v2 ingress with literal/CIDR peer admission, bounded metadata and classification, transparent TLS HTTP/1.1 and HTTP/2 capture, and cleartext HTTP/1.1/WebSocket capture.
- Typed target metadata, transparent setup failures, and bounded opaque TCP flow events with per-direction counts, truncation, half-close, and terminal semantics.
- A versioned language-neutral PROXY v2 conformance corpus and rootless guest-style forwarding integration for guest-agent and host-helper interoperability.

### Changed

- Proxy shutdown now cancels an active initial HTTP/2 SETTINGS wait promptly while retaining the bounded missing-SETTINGS deadline.
- HTTP/1.1 and HTTP/2 WebSockets now share the same internal capture session without changing the public event cases.
- Upstream connect, TLS negotiation, and HTTP/2 SETTINGS waits now use finite public timeout policy and cancellable setup ownership.
- Unknown TCP, ECH ClientHello traffic, and TLS offers without a supported HTTP ALPN now pass through byte-exact opaque forwarding instead of being dropped.
- Explicit and transparent ClientHello classification now performs bounded incremental work across arbitrary fragmentation without changing replay or routing semantics.

### Fixed

- Rejected HTTP/1.1 WebSocket upgrades no longer switch request capture into tunnel mode before a correlated valid `101`, so subsequent keep-alive HTTP remains structured and no spurious WebSocket events are emitted.
- Memory-sensitive test gates now fail on RSS sampling errors, and injected TLS-handler installation failures resolve every test handshake promise exactly once.

### Security

- Transparent destination metadata is accepted only from the configured actual peer, and SNI can select TLS identity but never redirect the original destination.
- Direct `/dev/pf` lookup remains host-helper-owned because the public macOS SDK does not expose a stable PF lookup ABI.

## 0.1.0 - 2026-08-08

### Added

- Embeddable HTTP `CONNECT` interception with dynamically minted P-256 TLS identities.
- Verified HTTP/1.1 and HTTP/2 forwarding with compatible origin-first ALPN negotiation.
- Structured HTTP and HTTP/1.1 WebSocket events with independently bounded body capture.
- Consumer-owned certificate-authority generation, export, restoration, and validation.
- Secure defaults for loopback binding, upstream certificate verification, internal egress denial, and metadata-only capture.
- Deterministic lifecycle behavior for package-owned and consumer-owned SwiftNIO event-loop groups.
- Bounded protocol parsing, certificate caching, capture retention, and streaming backpressure.
- Consumer integration guides, DocC coverage, security reporting, support guidance, and release automation.

### Security

- Resolved request-smuggling ambiguity across HTTP/1.1 framing and HTTP/2 pseudo-header conversion.
- Enforced egress policy on resolved candidates and the connected peer to resist DNS and connection-race bypasses.
- Rejected unusable restored CA material, invalid WebSocket wire lengths, malformed ClientHello input, and oversized protocol metadata.
- Ensured proxy shutdown closes and awaits accepted, upstream, and in-flight connections.

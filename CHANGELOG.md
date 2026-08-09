# Changelog

All notable SwiftMITM changes are recorded here. The project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/).

## Unreleased

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

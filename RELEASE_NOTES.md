# SwiftMITM 0.1.0

SwiftMITM 0.1.0 is the first public release of the standalone SwiftPM library for authorized TLS interception on macOS.

## Highlights

- Embeddable HTTP `CONNECT` proxy with dynamically minted P-256 leaf identities
- Verified HTTP/1.1 and HTTP/2 upstream connections with compatible ALPN negotiation
- Structured HTTP and HTTP/1.1 WebSocket capture events with bounded body retention
- Secure defaults for loopback binding, upstream TLS verification, internal egress, and body capture
- Deterministic lifecycle behavior for package-owned and consumer-owned SwiftNIO event-loop groups
- Bounded certificate caching, protocol parsing, capture buffering, and streaming backpressure
- Public restoration validation for consumer-persisted certificate-authority material
- Consumer integration guides and DocC coverage for the supported public API

## Requirements

- macOS 14 or newer
- Swift 6.0 or newer

## Known limitations

- HTTP/3 and QUIC are not supported.
- WebSocket over HTTP/2 extended `CONNECT` is not decoded.
- WebSocket `permessage-deflate` payloads are identified but not inflated.
- HTTP content encodings are forwarded and captured without automatic decompression.
- The embedding application owns proxy configuration, trust installation, sensitive storage, redaction, persistence, and presentation.

See the [README](https://github.com/ul0gic/swift-mitm/blob/0.1.0/README.md) for installation and the [DocC catalog](https://github.com/ul0gic/swift-mitm/blob/0.1.0/Sources/SwiftMITM/SwiftMITM.docc/SwiftMITM.md) for the complete consumer contract.

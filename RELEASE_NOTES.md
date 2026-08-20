# SwiftMITM 2.0.0

SwiftMITM 2.0.0 adds trusted transparent interception, bounded opaque TCP forwarding, and HTTP/2 WebSocket capture to the standalone SwiftPM library for authorized TLS interception on macOS.

## Highlights

- Trusted PROXY protocol v2 ingress with actual-peer literal/CIDR admission, bounded metadata and classification, and transparent TLS HTTP/1.1, HTTP/2, clear HTTP/1.1, and supported WebSocket capture.
- Bounded, byte-exact opaque TCP forwarding for ECH, unsupported ALPN, and other unclassified traffic, with per-direction retained-byte limits and terminal events.
- RFC 8441 WebSocket capture per HTTP/2 stream when the origin advertises extended CONNECT support; rejected upgrades remain ordinary HTTP capture.
- Typed destination, ingress, and original-client metadata plus typed transparent setup failures.
- Finite, cancellable upstream-connect, TLS-handshake, HTTP/2 SETTINGS, PROXY-header, and classification waits.
- A versioned PROXY v2 conformance corpus and rootless guest-style forwarder integration; guest agents and privileged host helpers own independent encoder proof and release-tagged artifact pinning.

## Requirements

- macOS 14 or newer
- Swift 6.0 or newer

## Migration from 0.1.0

- Explicit HTTP `CONNECT` remains the default ingress and existing construction remains valid.
- Exhaustive `CaptureEvent` switches must handle `opaqueOpen`, `opaqueData`, `opaqueDirectionEnd`, `opaqueClose`, `opaqueError`, and `connectionFailure`.
- `CapturedRequestHead.target` is optional for existing explicit flows. Consumers that construct it can omit the argument; consumers that inspect it must handle `nil`.
- Consumers that serialize capture events must preserve opaque-flow IDs, typed connection-failure reasons, and target metadata.
- Transparent ingress is opt-in. Its PROXY v2 metadata is trusted only after the direct TCP peer matches `TrustedPeerPolicy`; binding a listener does not grant trust.

## Known limitations

- UDP, QUIC, HTTP/3, cleartext HTTP/2 prior knowledge, and `h2c` upgrade are not supported.
- ECH decryption, general opaque-protocol decoding, WebSocket HTTP/2-to-HTTP/1.1 translation, payload inflation, message reassembly, and HTTP content decoding are not provided.
- Direct `/dev/pf` lookup is not provided. A host helper owns PF rules, privilege, tuple validation, cancellation, cleanup, and normalization into trusted PROXY v2.
- The embedding application owns authorization, listener access control, proxy and transparent-forwarding configuration, trust installation, sensitive storage, redaction, persistence, and presentation.

See the [README](https://github.com/ul0gic/swift-mitm/blob/2.0.0/README.md) for installation and the [DocC catalog](https://github.com/ul0gic/swift-mitm/blob/2.0.0/Sources/SwiftMITM/SwiftMITM.docc/SwiftMITM.md) for the complete consumer contract.

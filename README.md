# SwiftMITM

SwiftMITM is an embeddable Swift interception engine for authorized HTTP and TCP traffic. It accepts either explicit HTTP `CONNECT` traffic or a separately configured trusted PROXY protocol v2 ingress, preserves SwiftNIO backpressure, and emits structured HTTP, WebSocket, opaque-flow, and connection-failure events.

The package is the engine only. Your application owns authorization, CA-key persistence and trust, listener access control, guest or host forwarding, capture storage, redaction, and presentation.

## Status

SwiftMITM 0.2.0 is the current supported release. It retains explicit `CONNECT` as the default and adds trusted transparent ingress; integrations that exhaustively switch over `CaptureEvent` must migrate before adopting it.

## Capabilities

- Explicit HTTP `CONNECT` interception and trusted PROXY v2 transparent ingress with literal-address or CIDR peer admission
- TLS HTTP/1.1 and HTTP/2 capture through either ingress, with consumer-owned CA identities
- RFC 6455 WebSocket capture over HTTP/1.1 and RFC 8441 extended `CONNECT` capture over HTTP/2 through either ingress
- Transparent cleartext HTTP/1.1 and RFC 6455 WebSocket capture
- Bounded opaque TCP forwarding for ECH, unsupported ALPN, and other unclassified traffic
- Bounded HTTP bodies per direction, WebSocket payloads per frame, and opaque payloads per direction without truncating forwarding
- Verified upstream TLS, loopback binding, and internal-egress denial by default
- Explicit finite setup deadlines and deterministic proxy lifecycle ownership

## Requirements

- Swift 6.0 or newer
- macOS 14 or newer

## Installation

Install the current supported release:

```swift
dependencies: [
    .package(url: "https://github.com/ul0gic/swift-mitm.git", from: "0.2.0")
],
targets: [
    .target(
        name: "YourTarget",
        dependencies: [
            .product(name: "SwiftMITM", package: "swift-mitm")
        ]
    )
]
```

Complete the [0.2.0 migration](#capture-events-and-020-migration) before adopting transparent ingress or exhaustively switching on `CaptureEvent`.

In Xcode, use **File → Add Package Dependencies** and enter `https://github.com/ul0gic/swift-mitm.git`.

## Documentation

The [SwiftPM DocC catalog](Sources/SwiftMITM/SwiftMITM.docc/SwiftMITM.md) contains the complete consumer contract:

- [Getting Started](Sources/SwiftMITM/SwiftMITM.docc/GettingStarted.md)
- [Certificate Authority and Trust](Sources/SwiftMITM/SwiftMITM.docc/CertificateAuthorityAndTrust.md)
- [Capture Events](Sources/SwiftMITM/SwiftMITM.docc/CaptureEvents.md)
- [Lifecycle and Concurrency](Sources/SwiftMITM/SwiftMITM.docc/LifecycleAndConcurrency.md)
- [Security Model](Sources/SwiftMITM/SwiftMITM.docc/SecurityModel.md)

## PROXY v2 conformance contract

Language-neutral receiver and forwarder vectors live at [Conformance/ProxyV2/v1.json](Conformance/ProxyV2/v1.json). The 0.2.0 release artifact has SHA-256 `d9953424cbb63011c25820feca56ca60feb824b1db44dc4f82ef1940588fbf43`.

Guest agents and privileged host helpers must consume that exact release-tagged file and pin and verify its SHA-256 before using it for interoperability. SwiftMITM validates the receiver path and proves a local accepting forwarder without root. Each external repository owns its own PROXY v2 encoder implementation, CI, and evidence that it passes the emitter vectors; package CI cannot establish that proof for another repository.

The package does not provide a direct `/dev/pf` original-destination provider. The public macOS SDK has no stable PF lookup API for this use. A host helper owns PF rules, privilege, tuple validation, cancellation and cleanup, then normalizes its result into the trusted PROXY v2 contract.

See [the conformance contract](Conformance/ProxyV2/README.md) for the schema and role boundaries.

## Start an explicit CONNECT proxy

```swift
import SwiftMITM

struct DiscardingSink: CaptureEventSink {
    func receive(_: CaptureEvent) {}
}

let generated = try CertificateAuthority.generate(commonName: "My Tool MITM Root")
let proxy = ProxyServer(
    certificateAuthority: generated.authority,
    sink: DiscardingSink(),
    captureBodyLimit: 0
)

let port = try await proxy.start(port: 0)
try await proxy.stop()
```

This default ingress accepts HTTP `CONNECT`. Configure an authorized client to use `127.0.0.1:<port>` as its HTTP proxy. `start` allows one listener per `ProxyServer` instance; `stop` is idempotent, closes accepted and upstream channels, and waits for bounded setup work to quiesce. A package-owned event-loop group makes its proxy terminal after `stop`; a consumer-supplied group remains consumer-owned and permits a later restart.

## Start a trusted transparent listener

Transparent mode is a different ingress contract. Each accepted connection must come from an actual TCP peer admitted by `TrustedPeerPolicy`, then carry a supported PROXY v2 header. Binding to a non-loopback address merely permits the listener to accept network connections; it does not trust any peer. Keep the listener loopback-bound when a local host forwarder connects, or combine `allowNonLoopbackBind: true` with transport-level access control and a narrow trusted-peer policy.

```swift
let policy = TrustedPeerPolicy.loopback
if let transparentIngress = TrustedProxyV2Ingress(trustedPeers: policy) {
    let proxy = ProxyServer(
        certificateAuthority: generated.authority,
        sink: DiscardingSink(),
        ingress: .trustedProxyV2(transparentIngress),
        opaqueCaptureByteLimit: 0
    )
    let port = try await proxy.start(port: 0)
    try await proxy.stop()
    _ = port
}
```

`TrustedPeerPolicy(addressesAndCIDRs:)` accepts only non-empty IP literals and CIDRs. Its `.loopback` policy covers `127.0.0.0/8` and `::1/128`. PROXY metadata defaults to a 4 KiB header limit and a 5-second deadline. Untrusted peers and invalid, oversized, or late PROXY headers are closed and emit a typed connection-failure event.

Application classification has a separate 64 KiB limit and 1-second deadline. ECH, unsupported ALPN, classification timeout, and bounded non-TLS ambiguity fall back to opaque forwarding. A malformed or over-limit TLS ClientHello is closed with a classification-failure event.

One `ProxyServer` chooses one ingress for its single listener. Run separate instances when an application needs explicit `CONNECT` and transparent traffic simultaneously.

## Routing and protocol selection

In transparent mode, PROXY v2 supplies the original destination and original-client metadata. SwiftMITM routes upstream to that physical destination. A TLS SNI value refines only the upstream TLS name and the intercepted leaf identity; it never redirects the connection. Clear HTTP/1.1, TLS HTTP/1.1, TLS HTTP/2, RFC 6455, and RFC 8441 use the structured capture model. Opaque fallback preserves traffic without claiming to decode it.

`ProxyTimeoutPolicy` bounds upstream connection setup to 10 seconds, TLS handshakes to 10 seconds, and initial HTTP/2 SETTINGS to 5 seconds by default. Its failable initializer rejects zero, negative, and unrepresentable deadlines. `TrustedProxyV2Ingress` likewise rejects invalid bounds rather than silently widening resource limits.

## Capture events and 0.2.0 migration

`CaptureEventSink.receive(_:)` is synchronous. Return quickly: a slow sink stalls the owning event loop and applies backpressure. The same `Sendable` sink can be called concurrently by independent connections and HTTP/2 streams, so the consumer owns cross-stream synchronization and any bounded asynchronous handoff.

0.2.0 adds `CapturedTarget`, opaque-flow events, and connection-failure events. `CapturedRequestHead.target` remains optional for source-compatible value construction. Production requests through both ingress modes populate it with physical destination, logical authority, TLS server name when present, and ingress provenance; trusted transparent requests also include original-client metadata. Switches that exhaustively handle `CaptureEvent` must add:

- `opaqueOpen`, zero or more `opaqueData`, each direction's `opaqueDirectionEnd`, then exactly one `opaqueClose` on clean completion
- `opaqueError` instead of a clean close when opaque forwarding fails; no later terminal event follows
- `connectionFailure` for transparent ingress, classification, setup, TLS, transport, timeout, and cancellation failures before a structured or opaque flow owns the connection

Opaque `bytes` contains only retained bytes, while `byteCount` is the complete observed chunk size. `opaqueCaptureByteLimit` defaults to `0`, so the default emits metadata and exact counts while retaining no opaque payload bytes. The limit applies independently to client-to-server and server-to-client data; `opaqueDirectionEnd` reports each full direction count and whether retention was truncated. See the [Capture Events guide](Sources/SwiftMITM/SwiftMITM.docc/CaptureEvents.md) for the complete ordering contract.

## Security responsibilities

| Control | Default | Opt-in consequence |
| --- | --- | --- |
| Listener binding | Loopback only | `allowNonLoopbackBind: true` requires host-managed network access controls. |
| Transparent admission | Not enabled | A trusted PROXY v2 policy accepts only its actual peer addresses. |
| Upstream TLS | Certificate verification enabled | `verifyCertificate: false` removes upstream identity protection. |
| Egress policy | Internal, loopback, link-local, and unspecified addresses denied | `EgressPolicy(allowInternal: true)` permits local and private services. |
| Payload retention | Metadata only | Positive HTTP/WebSocket or opaque limits expose retained payload bytes to the sink. |

`additionalTrustRootsPEM` augments the system trust store. SwiftMITM neither installs CA trust nor configures proxy settings or transparent forwarding rules. Treat CA keys, traffic metadata, headers, and retained payloads as sensitive data.

## Current limitations

- No UDP, QUIC, HTTP/3, cleartext HTTP/2 prior knowledge, or `h2c` upgrade
- No ECH decryption, general opaque-protocol decoding, WebSocket HTTP/2-to-HTTP/1.1 translation, payload inflation, message reassembly, or content decoding
- Certificate pinning remains effective and can prevent interception
- No system proxy configuration, redirect-rule installation, privileged-helper lifecycle, Keychain integration, or trust UI

## Development

```sh
Scripts/verify.sh
```

The release gate runs format and strict lint checks, partitioned tests, a release build, and DocC validation. See [CHANGELOG.md](CHANGELOG.md), [CONTRIBUTING.md](CONTRIBUTING.md), and [RELEASING.md](RELEASING.md) for release and contribution policy.

## Support and security

Use [GitHub Issues](https://github.com/ul0gic/swift-mitm/issues) for reproducible defects and [SUPPORT.md](SUPPORT.md) for required diagnostic context. Report vulnerabilities privately according to [SECURITY.md](SECURITY.md).

## License

SwiftMITM is available under the [MIT License](LICENSE).

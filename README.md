# SwiftMITM

SwiftMITM is an embeddable TLS interception proxy engine written in Swift. It terminates authorized client TLS connections, establishes verified upstream connections, forwards traffic with SwiftNIO backpressure, and emits structured HTTP and WebSocket capture events to the embedding application.

The package is the interception engine only. Your application owns certificate persistence and trust, client proxy configuration, capture storage, filtering, redaction, and presentation.

## Status

SwiftMITM 0.1.0 is the initial public release. It follows Semantic Versioning, but its public API may evolve between minor releases before 1.0.

## Capabilities

- HTTP `CONNECT` proxying with per-host TLS certificates minted from a consumer-owned CA
- HTTP/1.1 and HTTP/2 forwarding with request and response capture events
- HTTP/1.1 WebSocket frame capture after a successful upgrade
- Independently bounded request and response body capture without truncating forwarded traffic
- Upstream certificate verification by default
- Loopback-only listening and internal-network egress denial by default
- Consumer-supplied SwiftNIO event-loop groups or package-owned lifecycle management

## Requirements

- Swift 6.0 or newer
- macOS 14 or newer

## Installation

For tagged releases, add the package with a version requirement:

```swift
dependencies: [
    .package(url: "https://github.com/ul0gic/swift-mitm.git", from: "0.1.0")
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

In Xcode, use **File → Add Package Dependencies** and enter `https://github.com/ul0gic/swift-mitm.git`.

## Documentation

The [SwiftPM DocC catalog](Sources/SwiftMITM/SwiftMITM.docc/SwiftMITM.md) contains the complete consumer contract:

- [Getting Started](Sources/SwiftMITM/SwiftMITM.docc/GettingStarted.md)
- [Certificate Authority and Trust](Sources/SwiftMITM/SwiftMITM.docc/CertificateAuthorityAndTrust.md)
- [Capture Events](Sources/SwiftMITM/SwiftMITM.docc/CaptureEvents.md)
- [Lifecycle and Concurrency](Sources/SwiftMITM/SwiftMITM.docc/LifecycleAndConcurrency.md)
- [Security Model](Sources/SwiftMITM/SwiftMITM.docc/SecurityModel.md)

## Minimal integration

Start with a sink that performs no work while proving listener, trust, and client configuration. Before retaining traffic, adopt the bounded handoff contract in the [Capture Events guide](Sources/SwiftMITM/SwiftMITM.docc/CaptureEvents.md).

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
```

`port: 0` selects an available local port. Retain the `ProxyServer` for the full interception session and stop it explicitly:

```swift
try await proxy.stop()
```

`start` rejects a second listener while the proxy is running. `stop` is idempotent, closes accepted and upstream channels, and waits for in-flight connection setup to quiesce. A proxy using its package-owned event-loop group is terminal after `stop`; a proxy using a consumer-supplied group can be started again, and SwiftMITM never shuts that supplied group down.

For clients that offer HTTP/2 and HTTP/1.1, SwiftMITM negotiates with the origin first and advertises only the compatible protocol to the intercepted client. An HTTP/1.1-only origin therefore remains usable by an HTTP/2-capable client without protocol translation.

Configure the authorized client to use `127.0.0.1:<port>` as its HTTP proxy. SwiftMITM does not modify system proxy settings.

## Certificate authority ownership

`CertificateAuthority.generate()` returns the live authority plus its P-256 private key and root certificate as PEM strings. The embedding application must protect and persist that material, install or remove trust through an appropriate user-controlled workflow, and restore the same authority when needed:

```swift
let authority = try CertificateAuthority(
    privateKeyPEM: storedPrivateKeyPEM,
    certificatePEM: storedCertificatePEM
)
```

Restoration validates PEM parsing, the certificate/private-key match, CA Basic Constraints, certificate-signing Key Usage, current validity, and the self-signature. Invalid material throws a specific `CertificateAuthorityRestorationError` before an unusable authority can enter service.

Each running proxy bounds completed leaf identities and distinct in-flight mints to 256 apiece, coalesces concurrent requests for the same hostname, and performs cache-miss certificate and TLS-context work outside SwiftNIO event loops. Stopping the proxy releases that per-run TLS runtime.

Never write the CA private key to logs, source control, defaults storage, or an unprotected file. Installing the root certificate as trusted allows certificates minted by that key to authenticate intercepted hosts until that trust is removed.

## Capture events

SwiftMITM emits:

- Request head, bounded body chunks, trailers, and request completion
- Informational and final response heads, bounded body chunks, trailers, and response completion
- Stream errors correlated by request ID
- WebSocket open, frame, and close events correlated by connection ID

Body events report both the captured bytes and the full observed chunk size. Completion events report whether the configured per-direction body limit truncated capture. A limit of `0`, the default, captures metadata without body bytes.

Event delivery is synchronous: `receive(_:)` completes inline before protocol processing continues. A slow or blocked sink stalls its owning event loop and applies backpressure, potentially delaying unrelated channels on that loop; SwiftMITM does not queue sink work. Per-stream order is head, body chunks, optional trailers, then end. The same `Sendable` sink may be called concurrently by independent connections or HTTP/2 streams, so consumers must provide cross-stream synchronization and must not assume global ordering. SwiftMITM does not recursively invoke the sink for one stream.

## Security defaults and opt-ins

| Control | Default | Opt-in consequence |
| --- | --- | --- |
| Listener binding | Loopback only | `allowNonLoopbackBind: true` can expose an open relay and requires external access controls |
| Upstream TLS | Certificate verification enabled | `verifyCertificate: false` removes upstream identity protection |
| Egress policy | Internal, loopback, link-local, and unspecified addresses denied | `EgressPolicy(allowInternal: true)` permits access to local and private services |
| Body capture | Metadata only | A positive `captureBodyLimit` exposes bounded payload bytes to the consumer |

`additionalTrustRootsPEM` augments the system trust store; it does not replace default roots.

Captured headers and bodies can contain credentials, cookies, tokens, and personal data. SwiftMITM does not redact, encrypt, or persist events. Those responsibilities belong to the consumer.

## Current limitations

- No HTTP/3 or QUIC interception
- No WebSocket-over-HTTP/2 extended `CONNECT` decoding
- `permessage-deflate` frames are marked compressed but emitted as unmasked compressed wire payloads
- No automatic Brotli or content-encoding decompression
- No system proxy configuration, Keychain integration, or certificate trust UI
- Certificate pinning remains effective and can prevent interception
- Public API may evolve between minor releases before `1.0`

## Development

```sh
Scripts/verify.sh
```

The release gate runs format and strict lint checks, debug and release builds, serial and parallel tests, and DocC validation. See [CHANGELOG.md](CHANGELOG.md) for release history, [CONTRIBUTING.md](CONTRIBUTING.md) for contribution requirements, and [RELEASING.md](RELEASING.md) for the tag contract.

## Support and security

Use [GitHub Issues](https://github.com/ul0gic/swift-mitm/issues) for reproducible defects and [SUPPORT.md](SUPPORT.md) for the required diagnostic context. Report vulnerabilities privately according to [SECURITY.md](SECURITY.md).

## Responsible use

Use SwiftMITM only with systems and traffic you own or are explicitly authorized to inspect. Do not expose the listener to untrusted networks, and treat CA keys and captured traffic as sensitive security material.

## License

SwiftMITM is available under the [MIT License](LICENSE).

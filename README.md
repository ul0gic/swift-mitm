# SwiftMITM

SwiftMITM is an embeddable TLS interception proxy engine written in Swift. It terminates authorized client TLS connections, establishes verified upstream connections, forwards traffic with SwiftNIO backpressure, and emits structured HTTP and WebSocket capture events to the embedding application.

The package is the interception engine only. Your application owns certificate persistence and trust, client proxy configuration, capture storage, filtering, redaction, and presentation.

## Status

SwiftMITM is functional but pre-release. The current source version is `0.0.1-spike`; no stable tag or compatibility guarantee exists yet. Use the `main` branch only for evaluation while the public API and lifecycle contracts are finalized.

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

Until the first release is tagged, add the package from `main`:

```swift
dependencies: [
    .package(url: "https://github.com/ul0gic/swift-mitm.git", branch: "main")
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

## Minimal integration

`CaptureEventSink.receive(_:)` is synchronous and runs on network-processing paths. Return immediately and move persistence, decoding, or other blocking work onto infrastructure your application owns.

```swift
import SwiftMITM

actor EventCounter {
    private var count = 0

    func record(_: CaptureEvent) {
        count += 1
    }
}

struct CountingSink: CaptureEventSink {
    let counter: EventCounter

    func receive(_ event: CaptureEvent) {
        Task {
            await counter.record(event)
        }
    }
}

let generated = try CertificateAuthority.generate(commonName: "My Tool MITM Root")
let counter = EventCounter()
let proxy = ProxyServer(
    certificateAuthority: generated.authority,
    sink: CountingSink(counter: counter),
    captureBodyLimit: 1_048_576
)

let port = try await proxy.start(port: 0)
```

`port: 0` selects an available local port. Retain the `ProxyServer` for the full interception session and stop it explicitly:

```swift
try await proxy.stop()
```

Configure the authorized client to use `127.0.0.1:<port>` as its HTTP proxy. SwiftMITM does not modify system proxy settings.

## Certificate authority ownership

`CertificateAuthority.generate()` returns the live authority plus its P-256 private key and root certificate as PEM strings. The embedding application must protect and persist that material, install or remove trust through an appropriate user-controlled workflow, and restore the same authority when needed:

```swift
let authority = try CertificateAuthority(
    privateKeyPEM: storedPrivateKeyPEM,
    certificatePEM: storedCertificatePEM
)
```

Never write the CA private key to logs, source control, defaults storage, or an unprotected file. Installing the root certificate as trusted allows certificates minted by that key to authenticate intercepted hosts until that trust is removed.

## Capture events

SwiftMITM emits:

- Request head, bounded body chunks, and request completion
- Response head, bounded body chunks, and response completion
- Stream errors correlated by request ID
- WebSocket open, frame, and close events correlated by connection ID

Body events report both the captured bytes and the full observed chunk size. Completion events report whether the configured per-direction body limit truncated capture. A limit of `0`, the default, captures metadata without body bytes.

Event delivery is synchronous. Ordering is preserved by the owning SwiftNIO channel or stream, but consumers must provide their own serialization when combining events across concurrent connections or HTTP/2 streams.

## Security defaults and opt-ins

| Control | Default | Opt-in consequence |
| --- | --- | --- |
| Listener binding | Loopback only | `allowNonLoopbackBind: true` can expose an open relay and requires external access controls |
| Upstream TLS | Certificate verification enabled | `verifyCertificate: false` removes upstream identity protection |
| Egress policy | Internal, loopback, link-local, and unspecified addresses denied | `EgressPolicy(allowInternal: true)` permits access to local and private services |
| Body capture | Metadata only | A positive `captureBodyLimit` exposes bounded payload bytes to the consumer |

When `additionalTrustRootsPEM` is nonempty, the current pre-release implementation replaces the default trust roots with the supplied certificates; augmentation semantics are not yet finalized.

Captured headers and bodies can contain credentials, cookies, tokens, and personal data. SwiftMITM does not redact, encrypt, or persist events. Those responsibilities belong to the consumer.

## Current limitations

- No HTTP/3 or QUIC interception
- No WebSocket-over-HTTP/2 extended `CONNECT` decoding
- `permessage-deflate` frames are marked compressed but emitted as unmasked compressed wire payloads
- No automatic Brotli or content-encoding decompression
- No system proxy configuration, Keychain integration, or certificate trust UI
- Certificate pinning remains effective and can prevent interception
- Public API and repeated lifecycle semantics may change before `0.1.0`

## Development

```sh
swift build
swift test
swiftformat Sources Tests Package.swift --lint --config .swiftformat
swiftlint lint --strict --quiet --config .swiftlint.yml
```

## Responsible use

Use SwiftMITM only with systems and traffic you own or are explicitly authorized to inspect. Do not expose the listener to untrusted networks, and treat CA keys and captured traffic as sensitive security material.

## Origin

SwiftMITM began as the interception engine for [API Ghost](https://github.com/ul0gic/api-ghost) and is being developed as an independent package for the Swift community.

## License

SwiftMITM is available under the MIT License.

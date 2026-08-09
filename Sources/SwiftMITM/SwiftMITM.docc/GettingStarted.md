# Getting Started

Create a certificate authority, supply a capture sink, start a loopback proxy, configure an authorized client, and stop the proxy explicitly.

## Overview

### Add the package

SwiftMITM requires Swift 6.0 or newer and macOS 14 or newer. Use a tagged release requirement:

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

### Create the minimum integration

The following sink deliberately discards events. It is useful for proving listener, trust, and client configuration before adding a bounded event-consumption path.

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
print("Configure the authorized client to use 127.0.0.1:\(port)")

try await proxy.stop()
```

Persist both `generated.privateKeyPEM` and `generated.certificatePEM` before relying on the authority across launches. Install the root certificate through an explicit, user-controlled host-application workflow, then configure the authorized client to use `127.0.0.1:<port>` as its HTTP proxy. SwiftMITM neither changes system proxy settings nor installs trust.

Retain ``ProxyServer`` for the entire interception session. A real application normally starts once, keeps serving while its own session is active, and awaits ``ProxyServer/stop()`` during shutdown.

### Restore an existing authority

Restore the same key and certificate that the application previously persisted:

```swift
let authority = try CertificateAuthority(
    privateKeyPEM: storedPrivateKeyPEM,
    certificatePEM: storedCertificatePEM
)
```

Restoration fails with ``CertificateAuthorityRestorationError`` when the material cannot safely issue a valid leaf chain. See <doc:CertificateAuthorityAndTrust> for the validation and ownership contract.

### Choose configuration deliberately

| Setting | Default | Guidance |
| --- | --- | --- |
| Listener host | `127.0.0.1` | Keep the loopback default unless the host application supplies authentication and network access controls. |
| Listener port | Required; `0` is allowed | Use `0` when the operating system should select an available port. |
| `captureBodyLimit` | `0` | `0` captures metadata only. A positive value is an independent per-direction byte limit and never truncates forwarded traffic. |
| `upstreamPolicy` | Verified system trust | Keep verification enabled. Additional roots augment, rather than replace, system roots. |
| `egressPolicy` | Internal destinations denied | Opt in only for authorized local-service inspection. |
| Event-loop group | Package owned | Supply a group only when the application needs to own and reuse its networking runtime. |
| `targetWindowSize` | `65,535` | Keep the default unless HTTP/2 flow-control tuning is measured and required. |

Origin ALPN is negotiated before client TLS completes. A client offering HTTP/2 and HTTP/1.1 can therefore use an HTTP/1.1-only origin without an HTTP translation layer.

### Diagnose initial integration failures

- A client trust error usually means the generated root is not trusted in that client's trust context, the restored authority differs from the installed root, or certificate pinning is active.
- ``ProxyServerError/nonLoopbackBindRejected(_:)`` means the requested listener is outside loopback without the explicit opt-in.
- An internal or local origin is denied by default; use `EgressPolicy(allowInternal: true)` only for an authorized environment.
- Metadata without body bytes is expected when `captureBodyLimit` is `0`.
- ``ProxyServerError/alreadyRunning`` means the same instance already owns a listener.
- ``ProxyServerError/eventLoopGroupShutdown`` means a package-owned proxy was stopped and is terminal; construct a new instance.

Next, define event handling with <doc:CaptureEvents>, then review <doc:LifecycleAndConcurrency> and <doc:SecurityModel> before shipping.

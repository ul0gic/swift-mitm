# Getting Started

Create a certificate authority, choose one ingress for one listener, start the proxy, and stop it explicitly.

## Add the package

SwiftMITM requires Swift 6.0 or newer and macOS 14 or newer.

The current supported release is 2.0.0:

```swift
dependencies: [
    .package(url: "https://github.com/ul0gic/swift-mitm.git", from: "2.0.0")
]
```

Review <doc:CaptureEvents#Migrate-exhaustive-event-handling-for-2.0.0> before adopting transparent ingress or exhaustively switching on ``CaptureEvent``.

## Start an explicit CONNECT listener

Explicit `CONNECT` is the default and preserves existing proxy-client integrations.

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

Persist `generated.privateKeyPEM` and `generated.certificatePEM` together before relying on the authority across launches. Install the root through an explicit user-controlled application workflow, then configure the authorized client to use the listener as an HTTP proxy. SwiftMITM does not install trust or change proxy settings.

## Start a trusted PROXY v2 listener

Use transparent ingress only when the component that connects to SwiftMITM is known and can be authenticated by its actual peer address. `TrustedPeerPolicy` evaluates the direct TCP peer, not the source address supplied in PROXY metadata. A listener bind address only controls where it listens; it grants no trust.

```swift
let trustedPeers = TrustedPeerPolicy.loopback
if let ingress = TrustedProxyV2Ingress(trustedPeers: trustedPeers) {
    let proxy = ProxyServer(
        certificateAuthority: generated.authority,
        sink: DiscardingSink(),
        ingress: .trustedProxyV2(ingress),
        opaqueCaptureByteLimit: 0
    )

    let port = try await proxy.start(port: 0)
    try await proxy.stop()
    _ = port
}
```

`TrustedPeerPolicy(addressesAndCIDRs:)` accepts only IP literals and CIDRs. `.loopback` is `127.0.0.0/8` plus `::1/128`. `TrustedProxyV2Ingress` defaults to a 4 KiB PROXY header limit and 5-second deadline, then a 64 KiB classification limit and 1-second deadline. Its failable initializer rejects invalid limits or deadlines.

Run a second `ProxyServer` instance for a second ingress. Each instance has exactly one listener and selects either `.explicitConnect` or `.trustedProxyV2` for that listener.

## Choose timeouts and capture limits

`ProxyTimeoutPolicy` defaults to 10 seconds for upstream connection setup, 10 seconds for TLS handshake completion, and 5 seconds for initial HTTP/2 SETTINGS. Supply a policy only to narrow or deliberately extend those finite deadlines; zero, negative, and unrepresentable durations are rejected.

`captureBodyLimit` defaults to `0`, retaining HTTP metadata without request or response body bytes. `opaqueCaptureByteLimit` also defaults to `0`, retaining opaque metadata and full byte counts without opaque payload bytes. Positive limits are independent in each direction and do not truncate forwarded traffic.

## Understand transparent routing

The trusted PROXY v2 destination is the physical upstream route. SwiftMITM does not let TLS SNI redirect that route. When SNI is present, it supplies the upstream TLS name and the intercepted leaf identity; when absent, the original destination is used. Clear HTTP/1.1, interceptable TLS HTTP/1.1, interceptable TLS HTTP/2, and supported WebSocket traffic enter structured capture. ECH, unsupported ALPN, and remaining unclassified traffic use opaque TCP forwarding.

## Stop and restart

Retain ``ProxyServer`` for the interception session. `stop()` is idempotent and closes the listener, accepted channels, upstream channels, and bounded setup work before it returns. A proxy with a package-owned event-loop group is terminal after stopping. A proxy using a consumer-supplied group can be started again after stopping; SwiftMITM never shuts down that supplied group.

Next, define event consumption with <doc:CaptureEvents>, then review <doc:LifecycleAndConcurrency> and <doc:SecurityModel> before shipping.

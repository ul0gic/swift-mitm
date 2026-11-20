# Getting Started

Create a certificate authority, choose one ingress for one listener, start the proxy, and stop it explicitly.

## Add the package

The package requires Swift tools 6.4 and targets macOS 26 or later. The project's pull requests build with Xcode 27.1.

Set the package dependency's minimum version to 0.2.4.

```swift
dependencies: [
    .package(url: "https://github.com/ul0gic/swift-mitm.git", from: "0.2.4")
]
```

Add `.product(name: "SwiftMITM", package: "swift-mitm")` to the consuming target's dependencies.

Review <doc:CaptureEvents#Migrate-exhaustive-event-handling-for-0.2.0> before adopting transparent ingress or exhaustively switching on ``CaptureEvent``.

When upgrading from 0.2.1, review <doc:CaptureEvents#Migrate-from-0.2.1-to-0.2.2-or-later>. Add the `SwiftMITMInspection` product to your executable target when semantic decoding is needed; the `SwiftMITM` product alone supplies raw capture and forwarding.

## Start an explicit CONNECT listener

Explicit `CONNECT` is the default. This lifecycle example starts a listener and immediately stops it. For a listener that stays up while you send requests, run the `Examples/FirstCapture` executable.

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
print("Listening on 127.0.0.1:\(port)")
try await proxy.stop()
```

Persist `generated.privateKeyPEM` and `generated.certificatePEM` together before relying on the authority across launches. Install the root through an explicit user-controlled application workflow, then configure the authorized client to use the listener as an HTTP proxy. SwiftMITM does not install trust or change proxy settings.

## Start a trusted PROXY v2 listener

Use transparent ingress with a forwarder you control. `TrustedPeerPolicy` checks the forwarder's actual TCP peer address before accepting its PROXY metadata. The listener bind address determines where SwiftMITM accepts connections.

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

Run a second `ProxyServer` instance for a second ingress. Each instance has exactly one listener and selects one ingress for that listener.

## Forward ordinary explicit HTTP

Use opt-in `.explicitHTTP` for ordinary HTTP forwarding. It accepts HTTP/1.1 absolute-form `http` requests and HTTPS `CONNECT` on one listener. `.explicitConnect` remains the default and remains CONNECT-only. Ordinary requests are routed by the absolute request URI; this listener is a forward proxy, not an origin server. Unsupported or ambiguous requests are rejected before forwarding.

`ProxyInterceptionPolicy(passThroughAuthorities:)` selects exact normalized host-and-port authorities for opaque TCP forwarding. The default policy is empty. A matching CONNECT authority or trusted transparent destination selects pass-through before upstream setup. The proxy enforces egress policy and forwards the application bytes unchanged. For TLS traffic, the client negotiates with and authenticates the origin directly.

The external example package at `Examples/FirstCapture/` uses only public SwiftMITM imports. Its command form is `FirstCapture --ca-cert PATH --ca-key PATH` with optional flags `--port INT`, repeatable `--passthrough AUTHORITY`, `--capture-bytes INT`, `--async`, `--allow-internal`, and repeatable `--upstream-ca PATH`. With `--async`, the consumer queues up to 256 records and 1 MiB plus one in-flight encoded record. Overflow terminates delivery and marks active captures incomplete while proxy forwarding continues. The example attempts to emit a `deliveryLoss` record if output is writable and suppresses `stopped` after loss. On SIGINT, asynchronous delivery drains immediately writable output and cancels if it would block. SIGTERM cancels delivery.

## Choose timeouts and capture limits

`ProxyTimeoutPolicy` defaults to 10 seconds for upstream connection setup, 10 seconds for TLS handshake completion, and 5 seconds for initial HTTP/2 SETTINGS. Supply a policy only to narrow or deliberately extend those finite deadlines; zero, negative, and unrepresentable durations are rejected.

`captureBodyLimit` defaults to `0`, retaining HTTP metadata without request or response body bytes. `opaqueCaptureByteLimit` also defaults to `0`, retaining opaque metadata and full byte counts without opaque payload bytes. Positive limits are independent in each direction and do not truncate forwarded traffic.

## Understand transparent routing

The trusted PROXY v2 destination supplies the physical upstream route. When present, SNI supplies the upstream TLS name and the intercepted leaf identity. Without SNI, the proxy uses the original destination. Clear HTTP/1.1, interceptable TLS HTTP/1.1 and HTTP/2, and supported WebSockets enter structured capture. See <doc:SecurityModel> for the transparent classifier's opaque forwarding criteria.

Core capture reports streamed raw body chunks and HTTP trailers. The optional `SwiftMITMInspection` product adds incremental SSE, gRPC/gRPC-Web framing and status, descriptor-backed protobuf fields, supported content decoding and WebSocket message observations. Connect an `HTTPInspector` through `observationSink` instead of attempting to reconstruct semantic messages from retained raw-body prefixes.

## Stop and restart

Retain ``ProxyServer`` for the interception session. `stop()` is idempotent and closes the listener, accepted channels, upstream channels, and bounded setup work before it returns. A proxy with a package-owned event-loop group is terminal after stopping. A proxy using a consumer-supplied group can be started again after stopping; SwiftMITM never shuts down that supplied group.

Next, define event consumption with <doc:CaptureEvents>, then review <doc:LifecycleAndConcurrency> and <doc:SecurityModel> before shipping.

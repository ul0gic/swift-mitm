# Getting Started

Create a certificate authority, choose one ingress for one listener, start the proxy, and stop it explicitly.

## Add the package

The package requires Swift tools 6.4 and targets macOS 26 or later. Build with Xcode 27.1 and its bundled Swift 6.4 compiler; Xcode itself requires macOS 26.6 or later. Candidate builds and macOS 26/27 runtime checks remain unverified until GitHub Actions completes.

The latest published release is 0.2.1:

```swift
dependencies: [
    .package(url: "https://github.com/ul0gic/swift-mitm.git", from: "0.2.1")
]
```

Review <doc:CaptureEvents#Migrate-exhaustive-event-handling-for-0.2.0> before adopting transparent ingress or exhaustively switching on ``CaptureEvent``.

Version 0.2.1 is the latest published release. Version 0.2.2 is candidate preparation only. Review <doc:CaptureEvents#Migrate-to-the-0.2.2-candidate> before using candidate source.

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

Run a second `ProxyServer` instance for a second ingress. Each instance has exactly one listener and selects one ingress for that listener.

## Forward ordinary explicit HTTP

The candidate adds opt-in `.explicitHTTP`. It accepts HTTP/1.1 absolute-form `http` requests and HTTPS `CONNECT` on one listener. `.explicitConnect` remains the default and remains CONNECT-only. Ordinary requests are routed by the absolute request URI; this listener is a forward proxy, not an origin server. Unsupported or ambiguous requests are rejected before forwarding.

`ProxyInterceptionPolicy(passThroughAuthorities:)` selects exact normalized host-and-port authorities for opaque TCP forwarding. The default policy is empty. For a matching explicit CONNECT or trusted transparent destination, the client and origin retain end-to-end TLS; SwiftMITM does not capture the encrypted HTTP exchange. Selection does not bypass egress policy or origin verification, and setup failures never fall back to pass-through.

The external example package at `Examples/FirstCapture/` uses only public SwiftMITM imports. Its command form is `FirstCapture --ca-cert PATH --ca-key PATH` with optional flags `--port INT`, repeatable `--passthrough AUTHORITY`, `--capture-bytes INT`, `--async`, `--allow-internal`, and repeatable `--upstream-ca PATH`. `--async` uses a consumer-owned handoff bounded to 256 events and 1 MiB plus one in-flight encoded record; overflow terminates delivery and prints a `deliveryLoss` record marking active captures incomplete while proxy forwarding continues. On SIGINT, asynchronous delivery drains immediately writable output and cancels if it would block; on SIGTERM, it cancels delivery. Loss marks active captures incomplete and suppresses `stopped`. The example does not install CA trust or change system proxy settings.

## Choose timeouts and capture limits

`ProxyTimeoutPolicy` defaults to 10 seconds for upstream connection setup, 10 seconds for TLS handshake completion, and 5 seconds for initial HTTP/2 SETTINGS. Supply a policy only to narrow or deliberately extend those finite deadlines; zero, negative, and unrepresentable durations are rejected.

`captureBodyLimit` defaults to `0`, retaining HTTP metadata without request or response body bytes. `opaqueCaptureByteLimit` also defaults to `0`, retaining opaque metadata and full byte counts without opaque payload bytes. Positive limits are independent in each direction and do not truncate forwarded traffic.

## Understand transparent routing

The trusted PROXY v2 destination is the physical upstream route. SwiftMITM does not let TLS SNI redirect that route. When SNI is present, it supplies the upstream TLS name and the intercepted leaf identity; when absent, the original destination is used. Clear HTTP/1.1, interceptable TLS HTTP/1.1, interceptable TLS HTTP/2, and supported WebSocket traffic enter structured capture. ECH, unsupported ALPN, and remaining unclassified traffic use opaque TCP forwarding.

The candidate's HTTP/1.1 SSE and HTTP/2 gRPC paths capture streamed raw body chunks and HTTP trailers. SwiftMITM does not decode SSE events, protobuf messages, gRPC message boundaries, or compressed content. The configured candidate matrix covers curl, URLSession HTTP/1.1 and HTTP/2, WKWebView SSE/WebSocket, Chromium SSE/WebSocket, independent gRPC, and the external example; its results have not completed.

## Stop and restart

Retain ``ProxyServer`` for the interception session. `stop()` is idempotent and closes the listener, accepted channels, upstream channels, and bounded setup work before it returns. A proxy with a package-owned event-loop group is terminal after stopping. A proxy using a consumer-supplied group can be started again after stopping; SwiftMITM never shuts down that supplied group.

Next, define event consumption with <doc:CaptureEvents>, then review <doc:LifecycleAndConcurrency> and <doc:SecurityModel> before shipping.

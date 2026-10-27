# HTTP/3

`SwiftMITMHTTP3` provides a route-bound HTTP/3 proxy using Apple's SwiftNIO QUIC and HTTP/3 implementations. It terminates downstream TLS with a certificate minted by your `CertificateAuthority`, verifies the upstream identity, and exposes the same capture, observation and HTTP control interfaces as the core package.

## Package and platform

The library lives in `Packages/SwiftMITMHTTP3` and requires macOS 27. Core `SwiftMITM` remains macOS 26. Both resolve with Crypto 5; the companion uses narrowly patched, pinned Apple dependencies documented in [Patches](../Packages/SwiftMITMHTTP3/Patches/README.md).

The companion is a separate local Swift package within this checkout, not a product exported by the repository's root manifest. Add that package when integrating HTTP/3. The [public consumer](../Tests/Clients/Stage2/HTTP3/ProductionHost/Package.swift) shows the actual package relationship; its [host](../Tests/Clients/Stage2/HTTP3/ProductionHost/Sources/HTTP3ProductionHost/HostMain.swift) uses the public server API.

## Routing and trust

Create an `HTTP3FixedRoute` with a logical authority and a numeric destination address/port. The authority determines certificate identity and accepted incoming requests; the destination determines where the upstream UDP connection dials. These may use different ports. Requests cannot select an arbitrary origin through their authority.

`HTTP3ProxyConfiguration` takes the route, resource limits, upstream trust, egress policy, capture body limit and non-loopback bind permission. Defaults use system trust, the core egress policy, no body capture and loopback-only binding. `customPEM` replaces system trust with the supplied roots; it does not disable identity verification. Empty or invalid custom roots fail configuration.

Pass the configuration, certificate authority and capture sink to `HTTP3ProxyServer`. Optional observation/control sinks and `HTTPControlRuntime` connect semantic inspection and request/response decisions. An optional shared `ResourceAdmissionController` applies alongside the server's own limits.

`start(host:port:)` returns the bound port; port zero requests an ephemeral port. `stop()` drains and closes transport ownership, returning an optional control stop report. With a supplied event-loop group, the server can restart after a completed stop; an internally owned group is shut down and that instance cannot restart. A shutdown timeout retains cleanup ownership and reports `shutdownTimedOut` rather than declaring resources released.

Native validated downstream address migration retains the authorized upstream route and TLS identity. Upstream sessions are scoped to the downstream connection; there is no cross-client reuse or automatic request retry.

## Capture and controls

Captured connections use `fixedRouteHTTP3`; messages use `http3`. Ordered fields preserve received value bytes through `HTTPHeaderField.rawValueBytes`. Fields created from text encode as UTF-8. Header/trailer octets and duplicate ordering are preserved; QUIC packets and QPACK wire encodings are not captured verbatim.

The bridge streams request and response bodies, informational responses and trailers. Original/effective observations connect to the shared control runtime for edits, synthetic responses, breakpoints and fault policies. Observation body limits do not require buffering the entire forwarded body.

## Resource limits

| Setting | Default | Maximum |
|---|---:|---:|
| Combined downstream/upstream connections | 128 | 1,024 |
| Pending handshakes | 64 | 256 |
| Request streams per connection | 128 | 1,024 |
| Retained-byte budget | 64 MiB | 256 MiB |
| Handshake deadline | 10 seconds | 30 seconds |

Counts are ceilings, not guaranteed capacity: byte admission can bind first. Header sections have a 32 KiB bound and at most 1,024 fields. QPACK uses a zero-capacity dynamic table.

`resourceUsage()` reports current admission, native/parser/forwarding reservations, refusal counts and sampled native output peaks. Reservations are accounting values, not process RSS. `nativeOutputSampleComplete` identifies incomplete native sampling; a zero count does not substitute for that flag.

## Protocol boundary

This is explicit route-bound interception, not transparent QUIC redirection. It does not provide WebTransport, HTTP Datagrams, MASQUE, upstream HTTP/3 mutual TLS, WebSocket over HTTP/3 or automatic HTTP/1.1/HTTP/2 fallback. Certificate pinning can still prevent interception.

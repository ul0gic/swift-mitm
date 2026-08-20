# Security Model

Treat SwiftMITM as a privileged interception engine. Authorization, listener access control, forwarding topology, sensitive-data handling, and trust UX belong to the embedding application.

## Secure defaults

| Boundary | Default | Explicit opt-in and consequence |
| --- | --- | --- |
| Listener | Loopback only | `allowNonLoopbackBind: true` can expose an unauthenticated listener and requires host-managed access control. |
| Ingress | Explicit HTTP `CONNECT` | Trusted transparent ingress requires a PROXY v2 configuration and actual-peer admission. |
| Origin TLS | Certificate and hostname verification | `verifyCertificate: false` permits unauthenticated upstream TLS. |
| Origin egress | Internal, loopback, link-local, unspecified, and other special-purpose addresses denied | `EgressPolicy(allowInternal: true)` permits local and private services. |
| HTTP and opaque payload capture | Metadata only | Positive limits expose retained payload bytes to the sink. |

Additional upstream roots augment the system trust store. DNS answers are filtered before TLS connection attempts, and the connected peer address is checked again to cover resolution and connection races.

## Deploy transparent ingress safely

`allowNonLoopbackBind` and `TrustedPeerPolicy` solve different problems. The first permits a bind address; the second admits the direct TCP peer that provides PROXY v2 metadata. A non-loopback listener is not trusted merely because it is reachable, and a source address inside the PROXY header is not used to establish trust.

Use the narrowest literal-address or CIDR policy that reaches the local host or guest forwarder. Rejecting an untrusted peer happens before accepting its claimed destination. The package does not install redirect rules, run a privileged helper, configure a guest forwarder, or authenticate a host-network listener. Those are application and deployment responsibilities.

The trusted PROXY v2 destination remains the upstream route. TLS SNI is identity metadata only: it can select the upstream TLS name and intercepted leaf identity but cannot redirect a transparent connection. ECH and unsupported ALPN use opaque forwarding rather than decryption or protocol guessing.

## Bound resources and setup time

Transparent PROXY metadata is bounded to 4 KiB and 5 seconds by default. Application classification is bounded to 64 KiB and 1 second. `ProxyTimeoutPolicy` defaults to 10 seconds for upstream connection setup, 10 seconds for TLS handshakes, and 5 seconds for initial HTTP/2 SETTINGS. Invalid limits and deadlines are rejected by failable configuration initializers.

HTTP bodies, WebSocket frames, and opaque data have distinct retained-byte limits from forwarding. `0` retains metadata only. Opaque counts remain complete even when no opaque bytes are retained. SwiftMITM preserves streaming backpressure instead of accumulating unbounded payloads or sink work.

## Application responsibilities

The embedding application must:

- Establish that the operator is authorized to intercept the selected clients and destinations.
- Protect, persist, install, and remove CA material through an explicit user-controlled workflow.
- Configure and remove client proxy settings or transparent forwarding safely, including interrupted-session cleanup.
- Authenticate and authorize access to every non-loopback listener and narrowly control its actual upstream forwarder peers.
- Bound, redact, encrypt, retain, and delete capture events according to its policy.
- Keep payloads, credentials, cookies, authorization headers, CA private keys, and personal data out of logs and diagnostics.

SwiftMITM intentionally provides no UI, Keychain policy, system-proxy modification, redirect-rule installation, privileged-helper lifecycle, persistence, redaction, analytics, or remote control plane.

## Capture is observational

Structured capture and opaque metadata observe forwarded traffic. They are not application-firewall or access-control decisions. Do not base access control on an event that arrives after forwarding has begun.

## Protocol limits

- No UDP, QUIC, HTTP/3, cleartext HTTP/2 prior knowledge, or `h2c` upgrade
- No ECH decryption or general opaque-protocol decoding
- HTTP/2 WebSocket capture requires origin-enabled extended `CONNECT`; malformed candidates affect only their paired stream
- Negotiated WebSocket `permessage-deflate` remains compressed
- HTTP content encodings such as gzip, deflate, and Brotli are not decoded
- Certificate pinning can prevent interception and is not bypassed

Any broader listener, internal egress, disabled origin verification, expanded capture, or decoded-content path requires its own explicit authorization and resource policy.

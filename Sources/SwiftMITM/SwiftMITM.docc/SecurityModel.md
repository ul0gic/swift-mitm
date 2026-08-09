# Security Model

Treat SwiftMITM as a privileged interception engine and keep authorization, access control, sensitive-data handling, and trust UX in the embedding application.

## Overview

### Secure defaults

| Boundary | Default | Explicit opt-in and consequence |
| --- | --- | --- |
| Listener | Loopback only | `allowNonLoopbackBind: true` can expose an unauthenticated forward proxy; the host must add network access controls. |
| Origin TLS | Certificate and hostname verification | `verifyCertificate: false` permits unauthenticated upstream TLS. |
| Origin egress | Non-global, internal, loopback, link-local, unspecified, and other special-purpose addresses denied | `EgressPolicy(allowInternal: true)` permits the intercepted client to reach local and private services. |
| HTTP body capture | Metadata only | A positive `captureBodyLimit` exposes bounded payload bytes to the sink. |

Additional upstream roots augment the system trust store. DNS answers are filtered before connection attempts, and the connected peer address is checked again to defend against resolution and connection races.

### Application responsibilities

The embedding application must:

- Establish that the operator is authorized to intercept the selected client and destinations.
- Protect, persist, install, and remove CA material through an explicit user-controlled workflow.
- Configure and restore client proxy settings safely, including cleanup after crashes or interrupted sessions.
- Authenticate and authorize any access to a non-loopback listener.
- Bound, redact, encrypt, retain, and delete captured data according to its own policy.
- Keep payloads, credentials, cookies, authorization headers, CA private keys, and captured personal data out of logs and diagnostics.

SwiftMITM intentionally does not provide UI, Keychain policy, system proxy modification, persistence, redaction, analytics, or a remote control plane.

### Capture is observational

HTTP capture parsers observe forwarded traffic; they are not an application firewall or policy-enforcement boundary. Do not base access-control decisions on capture events that occur after protocol forwarding has begun.

Body capture bounds retained bytes independently from forwarding. Hostile protocol lengths, header metadata, ClientHello buffering, WebSocket payload capture, leaf-identity retention, and pending certificate work have explicit limits. Backpressure remains part of the forwarding contract under large or stalled streams.

### Known protocol limits

- HTTP/3 and QUIC are not intercepted.
- WebSocket over HTTP/2 extended `CONNECT` is forwarded as stream data without WebSocket events.
- Negotiated WebSocket `permessage-deflate` payloads remain compressed.
- HTTP content encodings such as gzip, deflate, and Brotli are not decoded.
- Certificate pinning can prevent interception and is not bypassed.

Do not weaken these boundaries implicitly in a host application. Any broader listener, internal egress, disabled origin verification, expanded capture, or decoded-content path needs its own explicit authorization and resource policy.

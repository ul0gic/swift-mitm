# ``SwiftMITM``

Embed an authorized TLS interception proxy with verified upstream connections, bounded capture, and explicit lifecycle ownership.

## Overview

SwiftMITM accepts HTTP `CONNECT` tunnels, terminates client TLS with host identities issued by a consumer-owned certificate authority, negotiates a compatible HTTP/1.1 or HTTP/2 connection with the origin, forwards traffic with SwiftNIO backpressure, and emits structured capture events.

The package owns interception mechanics. The embedding application owns authorization, CA persistence, trust installation and removal, client proxy configuration, event storage, redaction, filtering, and presentation.

> Important: Use SwiftMITM only for traffic and systems you own or are explicitly authorized to inspect. Treat the CA private key and captured traffic as sensitive credentials and data.

## Topics

### Integration

- <doc:GettingStarted>
- <doc:CertificateAuthorityAndTrust>
- <doc:CaptureEvents>
- <doc:LifecycleAndConcurrency>
- <doc:SecurityModel>

### Proxy

- ``ProxyServer``
- ``ProxyServer/UpstreamPolicy``
- ``ProxyServerError``
- ``EgressPolicy``

### Certificate authority

- ``CertificateAuthority``
- ``GeneratedAuthority``
- ``CertificateAuthorityRestorationError``

### Capture

- ``CaptureEventSink``
- ``CaptureEvent``
- ``CapturedRequestHead``
- ``CapturedResponseHead``
- ``CapturedWebSocketFrame``
- ``HTTPHeaderField``
- ``HTTPProtocolVersion``
- ``WebSocketDirection``
- ``WebSocketOpcode``

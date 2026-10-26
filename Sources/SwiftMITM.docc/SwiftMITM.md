# ``SwiftMITM``

Embed an authorized explicit or trusted-transparent interception proxy with verified upstream connections, bounded capture, and explicit lifecycle ownership.

## Overview

SwiftMITM accepts explicit HTTP `CONNECT` tunnels by default. Candidate source adds opt-in ordinary HTTP forwarding through `.explicitHTTP`. A separately configured trusted PROXY protocol v2 ingress accepts transparent connections only from actual peers admitted by the embedding application's policy. Supported traffic enters structured capture; ECH, unsupported ALPN, and other unclassified traffic receive bounded opaque TCP forwarding. Candidate acceptance and runtime support remain unverified.

The package owns interception mechanics. The embedding application owns authorization, CA persistence, trust installation and removal, client proxy configuration, transparent forwarding, listener access control, event storage, redaction, filtering, and presentation.

> Important: Use SwiftMITM only for traffic and systems you own or are explicitly authorized to inspect. Treat CA private keys and captured traffic as sensitive data.

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
- ``ProxyIngress``
- ``HTTP1CorrelationPolicy``
- ``ProxyInterceptionPolicy``
- ``TrustedPeerPolicy``
- ``TrustedProxyV2Ingress``
- ``ProxyTimeoutPolicy``
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
- ``CapturedTarget``
- ``CapturedNetworkEndpoint``
- ``CapturedIngressProvenance``
- ``CapturedOpaqueFlow``
- ``OpaqueFlowDirection``
- ``OpaqueFlowCloseReason``
- ``CapturedConnectionFailure``
- ``CapturedConnectionFailureReason``
- ``ConnectionFailureStage``
- ``ConnectionFailureCode``
- ``OpaqueForwardingReason``
- ``CapturedWebSocketFrame``
- ``HTTPHeaderField``
- ``HTTPProtocolVersion``
- ``WebSocketDirection``
- ``WebSocketOpcode``

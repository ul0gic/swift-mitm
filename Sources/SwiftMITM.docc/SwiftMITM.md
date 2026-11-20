# ``SwiftMITM``

Embed an authorized explicit or trusted-transparent interception proxy with verified upstream connections, bounded capture, and explicit lifecycle ownership.

## Overview

SwiftMITM accepts explicit HTTP `CONNECT` tunnels by default. Select `.explicitHTTP` to forward ordinary HTTP requests as well, or `.priorKnowledgeHTTP2` for a configured HTTP/2 destination. Trusted PROXY v2 ingress checks the forwarder's TCP peer address before accepting its destination metadata. The transparent classifier captures supported protocols and can forward other traffic as opaque TCP. See <doc:SecurityModel>.

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


### HTTP controls

- ``HTTPControlRuntime``
- ``HTTPControlConfiguration``
- ``HTTPRule``
- ``HTTPControlDecision``
- ``BreakpointHandle``
- ``HTTPControlEventSink``

### Replay and export

- ``ReplayRequest``
- ``ReplayResult``
- ``ReplayResponseSink``
- ``HARExporter``
- ``HARExchange``
- ``HARExportConfiguration``
- ``HAROutputWriter``

### Shared resources

- ``ResourceAdmissionController``
- ``ResourceAdmissionConfiguration``
- ``HTTP1ReusePolicy``

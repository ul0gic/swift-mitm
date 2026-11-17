# SwiftMITM documentation

Choose the guide for the task you are implementing. `SwiftMITM` owns transport, capture, HTTP controls, replay, and HAR export; `SwiftMITMInspection` adds semantic observations.

| Task | Guide |
|---|---|
| Run a proxy against a temporary HTTPS origin | [Local lab](LocalLab.md) |
| Integrate the package and manage listener lifecycle | [Getting started](../Sources/SwiftMITM.docc/GettingStarted.md) |
| Wire semantic inspection and load protobuf descriptors | [Embedding](Embedding.md) |
| Understand the inspection console and supported protocols | [Inspection](Inspection.md) |
| Pause/edit a request, replay it, or export HAR | [Active debugging](ActiveDebugging.md) |
| Persist CA identity and configure separate trust inputs | [Certificate authority and trust](../Sources/SwiftMITM.docc/CertificateAuthorityAndTrust.md) |
| Consume capture events without losing ordering or blocking forwarding | [Capture events](../Sources/SwiftMITM.docc/CaptureEvents.md) |
| Own event loops and stop the package cleanly | [Lifecycle and concurrency](../Sources/SwiftMITM.docc/LifecycleAndConcurrency.md) |
| Review deployment boundaries and host responsibilities | [Security model](../Sources/SwiftMITM.docc/SecurityModel.md) |

[FirstCapture](../Examples/FirstCapture/README.md) is the small JSON Lines consumer. [InterceptionLab](../Examples/InterceptionLab/) demonstrates both products, a breakpoint, replay, and export through public APIs. The DocC [module index](../Sources/SwiftMITM.docc/SwiftMITM.md) groups the core API symbols.

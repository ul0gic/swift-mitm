# Lifecycle and Concurrency

Choose event-loop ownership once, serialize lifecycle operations, and await shutdown before releasing application resources.

## Overview

### Event-loop ownership

``ProxyServer`` either creates its own SwiftNIO event-loop group or uses one supplied by the consumer.

| Construction | Stop behavior | Restart behavior |
| --- | --- | --- |
| No `group` supplied | `stop()` closes the listener and tunnels, stops the TLS runtime, and shuts down the package-owned group. | The instance is terminal after `stop()`; create another proxy. |
| Consumer supplies `group` | `stop()` closes only SwiftMITM listener, tunnel, and TLS resources. The group remains running and consumer-owned. | The same proxy can start again after `stop()` completes. |

Calling `stop()` before the first `start()` is also an ownership decision: it is a no-op for an injected group, while it shuts down a package-owned group and makes that proxy terminal. Repeated sequential `stop()` calls succeed.

### Lifecycle transitions

- A second `start()` while running throws ``ProxyServerError/alreadyRunning`` and leaves the existing listener intact.
- A failed bind returns the proxy to a startable state.
- Overlapping `start()` and `stop()` operations throw ``ProxyServerError/lifecycleOperationInProgress`` instead of racing ownership transitions.
- Restarting an instance whose package-owned group was shut down throws ``ProxyServerError/eventLoopGroupShutdown``.
- A non-loopback listener without explicit opt-in throws ``ProxyServerError/nonLoopbackBindRejected(_:)``.

Serialize lifecycle calls in the embedding application even though conflicting transitions fail deterministically. Keep one clear owner responsible for session start and stop.

### Shutdown guarantee

``ProxyServer/stop()`` stops accepting connections, closes the listener, closes active accepted and upstream channels, waits for in-flight connection setup and channel closure, stops the per-run TLS worker pool, and then returns. A returned `stop()` therefore marks the end of SwiftMITM network activity for that run.

Capture delivery already executing on an event loop must return before that loop can finish its work. A blocked sink can delay shutdown, which is another reason to keep ``CaptureEventSink/receive(_:)`` bounded and nonblocking.

### Concurrent capture delivery

The sink protocol is `Sendable` because different connections or HTTP/2 streams can invoke it concurrently. Synchronize shared consumer state. Per-stream event order is stable, but arrival order across streams is intentionally unspecified.

SwiftMITM keeps network state on SwiftNIO event loops and moves cache-miss certificate and TLS-context work to its per-run worker pool. Consumers must apply the same discipline to any persistence, decoding, or analysis triggered by capture events.

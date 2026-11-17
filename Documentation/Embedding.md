# Embed semantic inspection

Link both `SwiftMITM` and `SwiftMITMInspection` in your executable target; [InterceptionLab's manifest](../Examples/InterceptionLab/Package.swift) shows the product declarations. The transport product captures raw protocol events. The inspection product receives full observation bytes before raw capture-retention limits and emits semantic events on its own bounded worker.

## Wire one session

This complete executable runs a loopback explicit proxy for 30 seconds and reports capture-head and SSE-message counts after shutdown. Supply an existing matching interception CA certificate and key as its first two arguments. An optional third argument is a binary `google.protobuf.FileDescriptorSet`; omit it for SSE and schema-free observation. Configure your controlled client to use port 8081 and trust the interception CA during the session.

```swift
import Foundation
import Synchronization
import SwiftMITM
import SwiftMITMInspection

final class EventCounts: CaptureEventSink, InspectionEventSink {
    let counts = Mutex((heads: 0, sse: 0))

    func receive(_ event: CaptureEvent) {
        if case .requestHead = event {
            counts.withLock { $0.heads += 1 }
        }
    }

    func receive(_ event: InspectionEvent) {
        if case .sse = event.payload {
            counts.withLock { $0.sse += 1 }
        }
    }
}

enum DemoError: Error { case usage }

@main
struct InspectionDemo {
    static func main() async throws {
        let arguments = CommandLine.arguments
        guard arguments.count == 3 || arguments.count == 4 else {
            throw DemoError.usage
        }
        let certificate = try String(contentsOfFile: arguments[1], encoding: .utf8)
        let key = try String(contentsOfFile: arguments[2], encoding: .utf8)
        let authority = try CertificateAuthority(privateKeyPEM: key, certificatePEM: certificate)
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        let budget = try InspectionMemoryBudget(admission: admission)
        let registry = ProtobufSchemaRegistry(budget: budget, limits: try ProtobufSchemaLimits())
        let schema: ProtobufSchemaSnapshot?
        if arguments.count == 4 {
            let bytes = try Data(contentsOf: URL(fileURLWithPath: arguments[3]))
            schema = try await registry.load(descriptorBytes: Array(bytes))
        } else {
            schema = nil
        }
        let events = EventCounts()
        let inspector = try HTTPInspector(
            configuration: InspectionConfiguration(budget: budget, schema: schema),
            sink: events
        )
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: events,
            captureBodyLimit: 0,
            observationSink: inspector,
            admission: admission
        )
        let port = try await proxy.start(port: 8081)
        try FileHandle.standardOutput.write(contentsOf: Data("listening=\(port)\n".utf8))
        do {
            try await Task.sleep(for: .seconds(30))
        } catch {
            _ = await inspector.shutdown()
            try await proxy.stop()
            throw error
        }
        try await proxy.stop()
        let report = await inspector.drain()
        _ = await inspector.shutdown()
        let totals = events.counts.withLock { $0 }
        print("heads=\(totals.heads) sse=\(totals.sse) complete=\(report.drained && !report.observationLossDetected)")
    }
}
```

This executable keeps default egress restrictions and system-root origin verification. For the private HTTPS origin in the [local lab](LocalLab.md), explicitly supply `EgressPolicy(allowInternal: true)` and an `UpstreamPolicy` containing that origin's PEM trust root when constructing the proxy. The client still needs the interception CA. Do not disable verification to make a private origin work.

The callbacks only increment protected counters; they perform no file I/O or blocking asynchronous bridge. Replace them with your application's bounded event handoff when retaining observations. `captureBodyLimit: 0` does not disable semantic inspection. Output counts describe observed events; `complete=false` means the observation history is incomplete even when the client succeeded.

## Load and retain a schema

Generate the descriptor set with the protobuf compiler in your own build pipeline, including imported definitions. `ProtobufSchemaRegistry.load(descriptorBytes:)` admits and validates it, computes its digest, and returns an immutable snapshot. Keep that snapshot in `InspectionConfiguration(schema:)` for the inspector that should use it. Loading or selecting a different registry snapshot does not mutate an existing inspector's configuration.

The supported schema subset is proto2/proto3 messages, enums, maps, oneofs and service method input/output mappings. Editions, extensions, uninterpreted options and unsupported features fail explicitly. A message body without a usable schema can still produce gRPC framing/status observations; missing field interpretation is reported as `schemaUnavailable`.

`ProtobufSchemaLimits` defaults to 4 MiB descriptors, 128 files, 10,000 fields and depth 32. `ProtobufSchemaSnapshot.decode` has separate message/node/depth limits and requires a message name. The registry retains its current snapshot; old snapshots remain alive while consumers hold them. At most four live snapshots can be admitted by one registry. Release obsolete inspectors and snapshots before loading more versions.

## Own failures and shutdown

Configuration errors throw at construction. Descriptor admission and parsing errors throw while loading, before a replacement inspector is created. Malformed streamed content, unsupported coding and observation-budget exhaustion produce `InspectionEvent.Payload.diagnostic` events; inspect their typed payload rather than treating an HTTP success as proof of complete decoding.

Stop the proxy before draining observation work. `drain()` waits for admitted work without stopping admission; it returns a report on timeout or cancellation. Inspect `drained`, `callbackInFlight` and `observationLossDetected`. Then call `shutdown()` to stop admission and release queued/session state; shutdown may discard pending observation work, so it is not a substitute for a successful drain when completeness matters. Sinks must remain responsive throughout this sequence. The application owns final persistence and its own queued copies.

The same `ResourceAdmissionController` is passed to both proxy and inspection budget. This shares configured admission accounting; it does not measure process RSS or account for native decoder workspace. See [Inspection limits](Inspection.md#feature-and-compatibility-limits), [Lifecycle](../Sources/SwiftMITM.docc/LifecycleAndConcurrency.md) and [Security](../Sources/SwiftMITM.docc/SecurityModel.md).

For controls, replay and HAR, follow [Active debugging](ActiveDebugging.md) and the public [InterceptionLab implementation](../Examples/InterceptionLab/Sources/InterceptionLab/InterceptionLab.swift). These remain explicit host actions; semantic observations do not automatically edit or replay traffic.

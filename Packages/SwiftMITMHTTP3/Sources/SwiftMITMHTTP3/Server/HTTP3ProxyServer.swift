import NIOCore
import NIOPosix
import SwiftMITM

public final class HTTP3ProxyServer: Sendable {
    private let lifecycle: HTTP3ServerLifecycle

    public init(
        configuration: HTTP3ProxyConfiguration,
        certificateAuthority: CertificateAuthority,
        sink: any CaptureEventSink,
        group: (any EventLoopGroup)? = nil,
        observationSink: (any HTTPObservationSink)? = nil,
        controlRuntime: HTTPControlRuntime? = nil,
        controlEventSink: (any HTTPControlEventSink)? = nil,
        admission: ResourceAdmissionController? = nil
    ) {
        let local = ResourceAdmissionController(configuration: configuration.admissionConfiguration)
        let inputs = HTTP3ServerInputs(
            configuration: configuration, certificateAuthority: certificateAuthority, sink: sink,
            observationSink: observationSink, controlRuntime: controlRuntime, controlEventSink: controlEventSink
        )
        lifecycle = HTTP3ServerLifecycle(
            inputs: inputs,
            group: group ?? MultiThreadedEventLoopGroup(numberOfThreads: 1),
            ownsGroup: group == nil,
            resources: HTTP3ResourceTracker(local: local, shared: admission)
        )
    }

    @discardableResult
    public func start(host: String = "127.0.0.1", port: Int) async throws -> Int {
        try await lifecycle.start(host: host, port: port)
    }

    @discardableResult
    public func stop() async throws -> HTTPControlStopReport? {
        try await lifecycle.stop()
    }

    public func resourceUsage() async -> HTTP3ResourceUsage {
        await lifecycle.resourceUsage()
    }
}

struct HTTP3ServerInputs: Sendable {
    let configuration: HTTP3ProxyConfiguration
    let certificateAuthority: CertificateAuthority
    let sink: any CaptureEventSink
    let observationSink: (any HTTPObservationSink)?
    let controlRuntime: HTTPControlRuntime?
    let controlEventSink: (any HTTPControlEventSink)?
}

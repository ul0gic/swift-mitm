import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2InspectionEvents: InspectionEventSink {
    private let storage = NIOLockedValueBox<[InspectionEvent]>([])

    var events: [InspectionEvent] { storage.withLockedValue { $0 } }

    func receive(_ event: InspectionEvent) { storage.withLockedValue { $0.append(event) } }

    func wait(until predicate: ([InspectionEvent]) -> Bool) async throws -> [InspectionEvent] {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(5))
        while true {
            let current = events
            if predicate(current) {
                return current
            }
            guard clock.now < deadline else { throw Phase2FixtureError.deadlineExceeded }
            try await Task.sleep(for: .milliseconds(10))
        }
    }
}

struct Stage2WireContext {
    let proxy: ProxyServer
    let group: EventLoopGroup
    let origin: Release022WireOrigin
    let client: Channel
    let received: Release022WireObservation
    let raw: Release022Events
    let inspected: Stage2InspectionEvents
    let inspector: HTTPInspector

    func close() async throws {
        if client.isActive {
            try await client.close().get()
        }
        try await proxy.stop()
        origin.stop()
        let shutdown = await inspector.shutdown()
        XCTAssertTrue(shutdown.drained)
        try await group.shutdownGracefully()
    }
}

func stage2WithInspectionWire(
    controlRuntime: HTTPControlRuntime? = nil,
    controlEventSink: any HTTPControlEventSink? = nil,
    inspectionConfiguration: InspectionConfiguration? = nil,
    admission: ResourceAdmissionController? = nil,
    reusePolicy: HTTP1ReusePolicy? = nil,
    body: (Stage2WireContext) async throws -> Void
) async throws {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
    let origin = Release022WireOrigin(group: group)
    let raw = Release022Events()
    let inspected = Stage2InspectionEvents()
    let inspector = try HTTPInspector(
        configuration: try inspectionConfiguration ?? InspectionConfiguration(budget: InspectionMemoryBudget()),
        sink: inspected
    )
    let proxy = ProxyServer(
        certificateAuthority: try CertificateAuthority.generate().authority,
        sink: raw,
        group: group,
        egressPolicy: .init(allowInternal: true),
        ingress: .explicitHTTP,
        captureBodyLimit: 0,
        observationSink: inspector,
        controlRuntime: controlRuntime,
        controlEventSink: controlEventSink,
        admission: admission,
        http1ReusePolicy: reusePolicy
    )
    var client: Channel?
    do {
        try origin.start()
        let port = try await proxy.start(port: 0)
        let received = Release022WireObservation()
        let channel = try release022WireClient(group: group, port: port, observation: received)
        client = channel
        let context = Stage2WireContext(
            proxy: proxy,
            group: group,
            origin: origin,
            client: channel,
            received: received,
            raw: raw,
            inspected: inspected,
            inspector: inspector
        )
        try await body(context)
        try await context.close()
    } catch {
        let failure = error
        await stage2CleanupInspectionWire(
            client: client,
            proxy: proxy,
            origin: origin,
            inspector: inspector,
            group: group
        )
        throw failure
    }
}

func stage2WriteChunk(_ bytes: [UInt8], origin: Release022WireOrigin) throws {
    try origin.write(Array("\(String(bytes.count, radix: 16))\r\n".utf8) + bytes + [13, 10])
}

func stage2ResponseCount(_ events: [CaptureEvent]) -> Int {
    events.reduce(0) { count, event in
        if case let .responseBodyChunk(_, _, byteCount) = event {
            count + byteCount
        } else {
            count
        }
    }
}

private func stage2CleanupInspectionWire(
    client: Channel?,
    proxy: ProxyServer,
    origin: Release022WireOrigin,
    inspector: HTTPInspector,
    group: MultiThreadedEventLoopGroup
) async {
    do {
        if let client, client.isActive {
            try await client.close().get()
        }
        try await proxy.stop()
    } catch { XCTFail("Proxy cleanup failed: \(error)") }
    origin.stop()
    let shutdown = await inspector.shutdown()
    XCTAssertTrue(shutdown.drained)
    do { try await group.shutdownGracefully() }
    catch { XCTFail("Event loop cleanup failed: \(error)") }
}

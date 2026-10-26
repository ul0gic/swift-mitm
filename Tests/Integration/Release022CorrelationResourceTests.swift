import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Release022CorrelationResourceTests: XCTestCase {
    private struct Scenario {
        let methodLength: Int
        let count: Int
        let cycles: Int
    }

    func testCountCeilingThenOneOverFailsAllOutstandingIDs() async throws {
        try await correlation(Scenario(methodLength: 3, count: 1024, cycles: 0))
    }

    func testMethodByteCeilingThenOneOverFailsAllOutstandingIDs() async throws {
        try await correlation(Scenario(methodLength: 131_072, count: 8, cycles: 0))
    }

    func testCombinedCeilingsThenOneOverFailsAllOutstandingIDs() async throws {
        try await correlation(Scenario(methodLength: 1024, count: 1024, cycles: 0))
    }

    func testMaximumCorrelationDrainsAndRefillsOneHundredCycles() async throws {
        try await correlation(Scenario(methodLength: 1024, count: 1024, cycles: 100))
    }

    private func correlation(_ scenario: Scenario) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Release022WireOrigin(group: group)
        let sink = Release022Events()
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        let policy = try XCTUnwrap(HTTP1CorrelationPolicy(
            maximumPendingExchanges: 1024,
            maximumPendingMethodBytes: 1_048_576
        ))
        let proxy = ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            correlationPolicy: policy
        )
        var client: Channel?
        let before = try MachMemory.residentBytes()
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let received = Release022WireObservation()
            let channel = try release022WireClient(group: group, port: port, observation: received)
            client = channel
            try warm(channel: channel, origin: origin, received: received, sink: sink)
            try fill(scenario, channel: channel, origin: origin, received: received, sink: sink)
            let stalled = try MachMemory.residentBytes()
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(string: overRequest)))
            try received.waitForClose()
            try origin.received.waitForClose()
            try await proxy.stop()
            assertOverload(sink.events, count: scenario.count)
            origin.stop()
            try await group.shutdownGracefully()
            let after = try MachMemory.residentBytes()
            let report = "CORRELATION-RESOURCE count=\(scenario.count) "
                + "methodBytes=\(scenario.count * scenario.methodLength) cycles=\(scenario.cycles) "
                + "beforeRSS=\(before) stalledRSS=\(stalled) afterRSS=\(after) unfinishedIDs=0\n"
            FileHandle.standardError.write(Data(report.utf8))
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private let overRequest = "Z /over HTTP/1.1\r\nHost: localhost\r\n\r\n"
    private let response = Array("HTTP/1.1 204 No Content\r\n\r\n".utf8)

    private func warm(
        channel: Channel,
        origin: Release022WireOrigin,
        received: Release022WireObservation,
        sink: Release022Events
    ) throws {
        let header = Phase4ProxyV2Header.ipv4(
            source: [192, 0, 2, 1],
            destination: [127, 0, 0, 1],
            sourcePort: 50_001,
            destinationPort: origin.port
        )
        let request = Array("GET /warm HTTP/1.1\r\nHost: localhost\r\n\r\n".utf8)
        try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: header.bytes + request)))
        _ = try origin.received.waitForBytes(request.count)
        try origin.write(response)
        _ = try received.waitForBytes(response.count)
        _ = try sink.wait { self.ends($0).count == 1 }
        sink.reset()
        origin.received.reset()
        received.reset()
    }

    private func fill(
        _ scenario: Scenario,
        channel: Channel,
        origin: Release022WireOrigin,
        received: Release022WireObservation,
        sink: Release022Events
    ) throws {
        let method = String(repeating: "A", count: scenario.methodLength)
        let request = Array("\(method) /ceiling HTTP/1.1\r\nHost: localhost\r\n\r\n".utf8)
        let batch = (0 ..< scenario.count).flatMap { _ in request }
        for _ in 0 ..< scenario.cycles {
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: batch)))
            XCTAssertEqual(try origin.received.waitForBytes(batch.count), batch)
            _ = try sink.wait { self.heads($0).count == scenario.count }
            try origin.write((0 ..< scenario.count).flatMap { _ in self.response })
            _ = try received.waitForBytes(response.count * scenario.count)
            let events = try sink.wait { self.ends($0).count == scenario.count }
            XCTAssertEqual(Set(ends(events)), Set(heads(events).map(\.id)))
            XCTAssertTrue(errors(events).isEmpty)
            sink.reset()
            origin.received.reset()
            received.reset()
        }
        try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: batch)))
        XCTAssertEqual(try origin.received.waitForBytes(batch.count), batch)
        let events = try sink.wait { self.heads($0).count == scenario.count }
        XCTAssertTrue(errors(events).isEmpty)
        XCTAssertEqual(heads(events).reduce(0) { $0 + $1.method.utf8.count }, scenario.count * scenario.methodLength)
    }

    private func assertOverload(_ events: [CaptureEvent], count: Int) {
        XCTAssertEqual(heads(events).count, count)
        XCTAssertEqual(errors(events).count, count)
        XCTAssertEqual(Set(errors(events)), Set(heads(events).map(\.id)))
        XCTAssertTrue(ends(events).isEmpty)
        let failures = events.compactMap { event -> CapturedConnectionFailure? in
            if case let .connectionFailure(failure) = event {
                failure
            } else {
                nil
            }
        }
        XCTAssertEqual(failures.count, 1)
        XCTAssertEqual(failures.first?.code, .resourceLimit)
        XCTAssertEqual(failures.first?.stage, .capture)
    }

    private func heads(_ events: [CaptureEvent]) -> [CapturedRequestHead] {
        events.compactMap {
            if case let .requestHead(head) = $0 {
                head
            } else {
                nil
            }
        }
    }

    private func ends(_ events: [CaptureEvent]) -> [UUID] {
        events.compactMap {
            if case let .responseEnd(id, _) = $0 {
                id
            } else {
                nil
            }
        }
    }

    private func errors(_ events: [CaptureEvent]) -> [UUID] {
        events.compactMap {
            if case let .streamError(id, _) = $0 {
                id
            } else {
                nil
            }
        }
    }
}

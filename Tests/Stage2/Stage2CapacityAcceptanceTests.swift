import Foundation
import NIOCore
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2CapacityAcceptanceTests: XCTestCase {
    private let retainedBodyBytes = 16_384
    private let replacement = Data("capacity-witness".utf8)

    func testThirtyTwoFlowsCancelWithoutRetainedGrowth() async throws {
        try await exerciseCycles(flowCount: 32)
    }

    func testTwoHundredFiftySixFlowsCancelWithoutGrowth() async throws {
        try await exerciseCycles(flowCount: 256)
    }

    private func exerciseCycles(flowCount: Int) async throws {
        var quiescentSamples: [UInt64] = []
        for cycle in 0..<20 {
            let baseline = try MachMemory.residentBytes()
            let occupied = try await exerciseCycle(flowCount: flowCount)
            let quiescent = try MachMemory.residentBytes()
            quiescentSamples.append(quiescent)
            let line = "capacity flows=\(flowCount) cycle=\(cycle) baselineRSS=\(baseline) "
                + "occupiedRSS=\(occupied) quiescentRSS=\(quiescent)\n"
            FileHandle.standardError.write(Data(line.utf8))
        }
        let firstMedian = quiescentSamples.prefix(5).sorted()[2]
        let lastMedian = quiescentSamples.suffix(5).sorted()[2]
        let allowance = max(UInt64(16 * 1024 * 1024), firstMedian / 10)
        XCTAssertLessThanOrEqual(
            MachMemory.growth(from: firstMedian, to: lastMedian),
            allowance,
            "flows=\(flowCount) firstMedian=\(firstMedian) lastMedian=\(lastMedian) samples=\(quiescentSamples)"
        )
    }

    private func exerciseCycle(flowCount: Int) async throws -> UInt64 {
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration(
            maximumConnections: flowCount * 2,
            maximumStreams: flowCount,
            maximumConnectionsPerDestination: flowCount
        ))
        let budget = try InspectionMemoryBudget(admission: admission)
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "retain-until-complete",
                match: HTTPRuleMatch(phase: .request, path: "/capacity"),
                action: .replaceBody(
                    replacement,
                    maximumInputBytes: retainedBodyBytes + 1,
                    contentEncoding: nil,
                    overrideNoTransform: false
                )
            )
        ], limits: HTTPControlLimits(
            // Allows 512 transport reservations for 256 held requests and witness completion.
            maximumTasks: 128,
            maximumBodyBytes: 32_768,
            maximumRetainedBytes: 67_108_864
        )))
        var occupiedRSS: UInt64?
        try await stage2WithHTTPOrigin(
            runtime: runtime,
            admission: admission,
            inspectorConfiguration: InspectionConfiguration(budget: budget)
        ) { peer in
            occupiedRSS = try await exercisePopulation(
                peer: peer,
                runtime: runtime,
                admission: admission,
                flowCount: flowCount
            )
        }
        let usage = await runtime.resourceUsage()
        XCTAssertEqual(usage.pausedExchanges, 0)
        XCTAssertEqual(usage.callbackTasks, 0)
        XCTAssertEqual(usage.orphanedCallbacks, 0)
        XCTAssertEqual(usage.taskSlots, 0)
        XCTAssertEqual(usage.retainedBytes, 0)
        XCTAssertEqual(budget.retainedBytes, 0)
        XCTAssertEqual(admission.usage.connections, 0)
        XCTAssertEqual(admission.usage.streams, 0)
        XCTAssertEqual(admission.usage.replayAttempts, 0)
        XCTAssertEqual(admission.usage.retainedBytes, 0)
        return try XCTUnwrap(occupiedRSS)
    }

    private func exercisePopulation(
        peer: Stage2HTTPContext,
        runtime: HTTPControlRuntime,
        admission: ResourceAdmissionController,
        flowCount: Int
    ) async throws -> UInt64 {
        let clients = try await openPopulation(peer: peer, flowCount: flowCount)
        do {
            try await stage2Await {
                self.requestBodyCount(peer.raw.events) == flowCount * self.retainedBodyBytes
                    && admission.usage.streams == flowCount
            }
            XCTAssertEqual(release022Requests(peer.raw.events).count, flowCount)
            XCTAssertEqual(admission.usage.connections, flowCount)
            XCTAssertTrue(clients.allSatisfy { $0.channel.isActive && $0.observation.bytes.isEmpty })
            XCTAssertEqual(peer.origin.acceptedConnections, 0)
            XCTAssertTrue(peer.origin.requests.isEmpty)
            let held = await runtime.resourceUsage()
            XCTAssertGreaterThanOrEqual(held.retainedBytes, flowCount * retainedBodyBytes)
            XCTAssertGreaterThanOrEqual(admission.usage.retainedBytes, held.retainedBytes)
            XCTAssertTrue(release022Errors(peer.raw.events).isEmpty)
            let occupied = try MachMemory.residentBytes()
            try await completeWitness(try XCTUnwrap(clients.first), peer: peer)
            for client in clients where client.channel.isActive {
                try await client.channel.close().get()
            }
            try await stage2Await { admission.usage.streams == 0 && admission.usage.connections == 0 }
            let terminal = try peer.raw.wait { release022Errors($0).count == flowCount - 1 }
            let requests = release022Requests(terminal)
            let failures = release022Errors(terminal)
            XCTAssertEqual(failures.count, flowCount - 1)
            XCTAssertEqual(Set(failures).count, flowCount - 1)
            XCTAssertEqual(Set(failures).union(release022ResponseEnds(terminal)), Set(requests.map(\.id)))
            return occupied
        } catch {
            for client in clients where client.channel.isActive {
                do { try await client.channel.close().get() } catch { XCTFail("Capacity client cleanup: \(error)") }
            }
            throw error
        }
    }

    private struct Client {
        let channel: Channel
        let observation: Release022WireObservation
    }

    private func openPopulation(peer: Stage2HTTPContext, flowCount: Int) async throws -> [Client] {
        var clients: [Client] = []
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(30))
        do {
            for _ in 0..<flowCount {
                guard clock.now < deadline else { throw Phase2FixtureError.deadlineExceeded }
                let observation = Release022WireObservation()
                let channel = try release022WireClient(
                    group: peer.group,
                    port: peer.proxyPort,
                    observation: observation
                )
                clients.append(Client(channel: channel, observation: observation))
                let head = "POST http://127.0.0.1:\(peer.origin.port)/capacity HTTP/1.1\r\n"
                    + "Host: localhost\r\nContent-Length: \(retainedBodyBytes + 1)\r\n\r\n"
                var buffer = ByteBuffer(string: head)
                buffer.writeBytes([UInt8](repeating: 97, count: retainedBodyBytes))
                try await channel.writeAndFlush(buffer).get()
            }
            return clients
        } catch {
            for client in clients where client.channel.isActive {
                do { try await client.channel.close().get() } catch { XCTFail("Capacity setup cleanup: \(error)") }
            }
            throw error
        }
    }

    private func completeWitness(_ client: Client, peer: Stage2HTTPContext) async throws {
        try await client.channel.writeAndFlush(ByteBuffer(bytes: [98])).get()
        try await stage2Await { peer.origin.requests.count == 1 }
        XCTAssertEqual(peer.origin.requests.first?.body, Array(replacement))
        _ = try client.observation.wait { $0.starts(with: Array("HTTP/1.1 200 ".utf8)) }
        _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
    }

    private func requestBodyCount(_ events: [CaptureEvent]) -> Int {
        events.reduce(0) { total, event in
            if case let .requestBodyChunk(_, bytes, count) = event {
                XCTAssertTrue(bytes.isEmpty)
                return total + count
            }
            return total
        }
    }
}

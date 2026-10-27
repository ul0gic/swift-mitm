import Dispatch
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOPosix
import XCTest

import SwiftMITM
import SwiftMITMInspection

private final class AggregateInspectionGate: InspectionEventSink {
    private struct State { var entered = false; var released = false; var losses = 0 }
    private let state = NIOLockedValueBox(State())
    private let signal = DispatchSemaphore(value: 0)

    var entered: Bool { state.withLockedValue { $0.entered } }
    var losses: Int { state.withLockedValue { $0.losses } }

    func receive(_ event: InspectionEvent) {
        let block = state.withLockedValue { state in
            if case .diagnostic(.capacityExceeded) = event.payload {
                state.losses += 1
            }
            guard case .head = event.payload, !state.entered, !state.released else { return false }
            state.entered = true
            return true
        }
        if block {
            signal.wait()
        }
    }

    func release() {
        let wake = state.withLockedValue { state in
            guard !state.released else { return false }
            state.released = true
            return true
        }
        if wake {
            signal.signal()
        }
    }
}

private final class AggregateReceiver: Sendable {
    let completion: Phase2FixtureCompletion<Phase6DigestResult>
    private let count = NIOLockedValueBox(0)
    var received: Int { count.withLockedValue { $0 } }

    init(eventLoop: any EventLoop) {
        completion = Phase2FixtureCompletion(eventLoop: eventLoop, timeout: .seconds(180))
    }

    func record(_ bytes: Int) { count.withLockedValue { $0 += bytes } }
}

private final class AggregateResponseHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart
    private let receiver: AggregateReceiver
    private let originPort: Int
    private let accumulator: Phase6DigestAccumulator
    private var result: Phase6DigestResult?

    init(receiver: AggregateReceiver, originPort: Int, bodySize: Int) {
        self.receiver = receiver
        self.originPort = originPort
        accumulator = Phase6DigestAccumulator(expectedByteCount: bodySize)
    }

    func channelActive(context: ChannelHandlerContext) {
        let head = HTTPRequestHead(
            version: .http1_1,
            method: .GET,
            uri: "http://127.0.0.1:\(originPort)/aggregate",
            headers: ["Host": "localhost"]
        )
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case let .head(head) where head.status != .ok:
            receiver.completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
        case .head: break
        case let .body(buffer):
            do {
                result = try accumulator.append(buffer) ?? result
                receiver.record(buffer.readableBytes)
            } catch { errorCaught(context: context, error: error) }
        case .end:
            if let result {
                receiver.completion.complete(.success(result))
            } else {
                receiver.completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
            }
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        receiver.completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        receiver.completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

final class Stage2AggregateCapacityAcceptanceTests: XCTestCase {
    private static let sharedLimit = 1_073_741_824
    private static let localLimit = 268_435_456
    private static let bodySize = 134_217_728
    private static let chunkSize = 65_536

    private struct Leg {
        let proxy: ProxyServer
        let inspector: HTTPInspector
        let budget: InspectionMemoryBudget
        let gate: AggregateInspectionGate
        let capture: CountingSink
        let client: Channel
        let upstream: Channel
        let receiver: AggregateReceiver
    }

    func testSharedGiBCeilingRejectsObservationAndKeepsWire() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Release022WireOrigin(group: group)
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration(
            maximumRetainedBytes: Self.sharedLimit
        ))
        var legs: [Leg] = []
        do {
            try origin.start()
            let authority = try CertificateAuthority.generate().authority
            for index in 0..<5 {
                await legs.append(try makeLeg(
                    index: index,
                    origin: origin,
                    group: group,
                    authority: authority,
                    admission: admission
                ))
            }
            try await exerciseBoundary(legs: legs, admission: admission)
            try await close(legs)
            origin.stop()
            XCTAssertEqual(admission.usage.connections, 0)
            XCTAssertEqual(admission.usage.streams, 0)
            XCTAssertEqual(admission.usage.retainedBytes, 0)
            XCTAssertEqual(admission.usage.replayAttempts, 0)
            try await group.shutdownGracefully()
        } catch {
            for leg in legs { leg.gate.release() }
            do { try await close(legs) } catch { XCTFail("Aggregate cleanup: \(error)") }
            origin.stop()
            do { try await group.shutdownGracefully() } catch { XCTFail("Aggregate group cleanup: \(error)") }
            throw error
        }
    }

    private func makeLeg(
        index: Int,
        origin: Release022WireOrigin,
        group: MultiThreadedEventLoopGroup,
        authority: CertificateAuthority,
        admission: ResourceAdmissionController
    ) async throws -> Leg {
        let (budget, gate, inspector) = try makeObservation(admission: admission)
        let capture = CountingSink()
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: capture,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .explicitHTTP,
            captureBodyLimit: 0,
            observationSink: inspector,
            admission: admission
        )
        var client: Channel?
        do {
            let port = try await proxy.start(port: 0)
            let receiver = AggregateReceiver(eventLoop: group.next())
            let channel = try await connect(port: port, originPort: origin.port, group: group, receiver: receiver)
            client = channel
            let upstream = try origin.waitForConnection(at: index)
            _ = try upstream.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try await stage2Await { gate.entered }
            try await upstream.channel
                .writeAndFlush(ByteBuffer(string:
                    "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n"
                        + "Content-Length: \(Self.bodySize)\r\n\r\n"))
                .get()
            return Leg(
                proxy: proxy,
                inspector: inspector,
                budget: budget,
                gate: gate,
                capture: capture,
                client: channel,
                upstream: upstream.channel,
                receiver: receiver
            )
        } catch {
            gate.release()
            if let client, client.isActive {
                do { try await client.close().get() } catch { XCTFail("Aggregate setup client: \(error)") }
            }
            do { try await proxy.stop() } catch { XCTFail("Aggregate setup proxy: \(error)") }
            let shutdown = await inspector.shutdown()
            XCTAssertTrue(shutdown.drained)
            throw error
        }
    }

    private func makeObservation(
        admission: ResourceAdmissionController
    ) throws -> (InspectionMemoryBudget, AggregateInspectionGate, HTTPInspector) {
        let budget = try InspectionMemoryBudget(maximumBytes: Self.localLimit, admission: admission)
        let gate = AggregateInspectionGate()
        let inspector = try HTTPInspector(configuration: InspectionConfiguration(
            budget: budget,
            maximumQueuedBytes: Self.localLimit,
            maximumQueuedItems: 65_536,
            maximumActiveBodies: 2
        ), sink: gate)
        return (budget, gate, inspector)
    }

    private func connect(
        port: Int,
        originPort: Int,
        group: MultiThreadedEventLoopGroup,
        receiver: AggregateReceiver
    ) async throws -> Channel {
        try await ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers([
                        HTTPRequestEncoder(),
                        ByteToMessageHandler(HTTPResponseDecoder()),
                        AggregateResponseHandler(receiver: receiver, originPort: originPort, bodySize: Self.bodySize)
                    ])
                }
            }
            .connect(host: "127.0.0.1", port: port)
            .get()
    }

    private func exerciseBoundary(legs: [Leg], admission: ResourceAdmissionController) async throws {
        let clock = ContinuousClock()
        let started = clock.now
        let deadline = started.advanced(by: .seconds(150))
        let chunk = ByteBuffer(bytes: [UInt8](repeating: 97, count: Self.chunkSize))
        let baseline = try MachMemory.residentBytes()
        var sent = [Int](repeating: 0, count: legs.count)
        var rejected: Int?
        var phase = "fill"
        var queuedItems = 0
        defer {
            let line = "aggregate phase=\(phase) elapsed=\(started.duration(to: clock.now)) sent=\(sent) "
                + "received=\(legs.map { $0.receiver.received }) sharedBytes=\(admission.usage.retainedBytes) "
                + "localBytes=\(legs.map { $0.budget.retainedBytes }) lastQueueItems=\(queuedItems) "
                + "blocked=\(legs.map { $0.gate.entered }) "
                + "losses=\(legs.map { $0.inspector.observationLossDetected })\n"
            FileHandle.standardError.write(Data(line.utf8))
        }
        while rejected == nil {
            for index in legs.indices {
                phase = "fill-check-\(index)"
                guard clock.now < deadline, sent[index] < Self.bodySize else {
                    throw Phase2FixtureError.deadlineExceeded
                }
                let before = admission.usage.retainedBytes
                let local = await legs[index].inspector.drain(timeout: .zero)
                queuedItems = local.queuedItems
                phase = "fill-write-\(index)"
                try await legs[index].upstream.writeAndFlush(chunk).get()
                sent[index] += Self.chunkSize
                let expected = sent[index]
                phase = "fill-receive-\(index)"
                try await stage2Await { legs[index].receiver.received == expected }
                XCTAssertLessThanOrEqual(admission.usage.retainedBytes, Self.sharedLimit)
                if legs[index].inspector.observationLossDetected {
                    var locals = [local]
                    for other in legs.indices where other != index {
                        await locals.append(legs[other].inspector.drain(timeout: .zero))
                    }
                    try recordBoundary(before: before, locals: locals, sent: sent, baseline: baseline)
                    rejected = index
                    break
                }
            }
        }
        phase = "post-refusal"
        let failed = try XCTUnwrap(rejected)
        for leg in legs { leg.gate.release() }
        try await stage2Await { legs[failed].gate.losses == 1 }
        try await finishBodies(legs: legs, sent: sent, chunk: chunk, deadline: deadline)
    }

    private func recordBoundary(
        before: Int,
        locals: [InspectionDrainResult],
        sent: [Int],
        baseline: UInt64
    ) throws {
        // Local byte/item headroom isolates the shared limit; the extra unit is one wire chunk.
        XCTAssertGreaterThan(before, Self.sharedLimit - 1_048_576)
        XCTAssertLessThanOrEqual(before, Self.sharedLimit)
        for local in locals {
            XCTAssertLessThan(local.retainedBytes, Self.localLimit - 1_048_576)
            XCTAssertLessThan(local.queuedItems, 60_000)
            XCTAssertTrue(local.callbackInFlight)
        }
        XCTAssertTrue(sent.allSatisfy { $0 > 0 })
        let occupied = try MachMemory.residentBytes()
        let queuedPayload = sent.reduce(0, +) - Self.chunkSize
        let line = "aggregate sharedLimit=\(Self.sharedLimit) beforeRefusal=\(before) "
            + "localBytes=\(locals.map(\.retainedBytes)) queuedItems=\(locals.map(\.queuedItems)) "
            + "queuedPayloadBeforeRefusal=\(queuedPayload) forwardedBodyBytes=\(sent) "
            + "baselineRSS=\(baseline) refusalRSS=\(occupied)\n"
        FileHandle.standardError.write(Data(line.utf8))
    }

    private func finishBodies(
        legs: [Leg],
        sent: [Int],
        chunk: ByteBuffer,
        deadline: ContinuousClock.Instant
    ) async throws {
        var completed = sent
        let clock = ContinuousClock()
        while completed.contains(where: { $0 < Self.bodySize }) {
            guard clock.now < deadline else { throw Phase2FixtureError.deadlineExceeded }
            for index in legs.indices where completed[index] < Self.bodySize {
                try await legs[index].upstream.writeAndFlush(chunk).get()
                completed[index] += Self.chunkSize
                let expected = completed[index]
                try await stage2Await { legs[index].receiver.received == expected }
            }
        }
        let expectedDigest = Phase6ResourceStream.digest(repeating: 97, count: Self.bodySize)
        for leg in legs {
            let received = try await leg.receiver.completion.futureResult.get()
            XCTAssertEqual(received.byteCount, Self.bodySize)
            XCTAssertEqual(received.digest, expectedDigest)
            XCTAssertEqual(leg.capture.capturedResponseBytes, Self.bodySize)
            XCTAssertEqual(leg.capture.capturedErrors, 0)
        }
        for leg in legs {
            let drained = await leg.inspector.drain()
            XCTAssertTrue(drained.drained)
        }
    }

    private func close(_ legs: [Leg]) async throws {
        for leg in legs { leg.gate.release() }
        var firstFailure: any Error?
        for leg in legs {
            do {
                if leg.client.isActive {
                    try await leg.client.close().get()
                }
            } catch {
                if firstFailure == nil {
                    firstFailure = error
                }
            }
            do { try await leg.proxy.stop() } catch {
                if firstFailure == nil {
                    firstFailure = error
                }
            }
            let stopped = await leg.inspector.shutdown()
            XCTAssertTrue(stopped.drained)
            XCTAssertEqual(leg.budget.retainedBytes, 0)
        }
        if let firstFailure {
            throw firstFailure
        }
    }
}

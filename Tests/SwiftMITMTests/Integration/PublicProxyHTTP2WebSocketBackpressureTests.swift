import Foundation
import NIOPosix
import XCTest

import SwiftMITM

private let phase3WebSocketLoadSemaphore = DispatchSemaphore(value: 1)

private final class Phase3WebSocketLoadPermit: @unchecked Sendable {
    static func acquire() async -> Phase3WebSocketLoadPermit {
        await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                phase3WebSocketLoadSemaphore.wait()
                continuation.resume(returning: Phase3WebSocketLoadPermit())
            }
        }
    }

    deinit {
        phase3WebSocketLoadSemaphore.signal()
    }
}

private struct Phase3WebSocketLoadMeasurement {
    let stalledBytes: Int
    let rssGrowth: UInt64
    let transferSeconds: TimeInterval
    let snapshot: WebSocketRecordingSink.Snapshot

    var context: String {
        let frame = snapshot.frames.only
        return "RFC8441-BACKPRESSURE stalledBytes=\(stalledBytes) rssDelta=\(rssGrowth) "
            + "transferSeconds=\(String(format: "%.3f", transferSeconds)) "
            + "byteCount=\(frame?.byteCount ?? 0) retained=\(frame?.bytes.count ?? 0) "
            + "opens=\(snapshot.opens.count) frames=\(snapshot.frames.count) "
            + "closes=\(snapshot.closes.count) errors="
            + "\(snapshot.eventKinds.filter { $0 == .streamError }.count)"
    }
}

final class PublicProxyHTTP2WebSocketBackpressureTests: XCTestCase {
    func testOneMiBRFC8441FrameCompletesTerminalLifecycleWithoutStall() async throws {
        let permit = await Phase3WebSocketLoadPermit.acquire()
        defer { withExtendedLifetime(permit) {} }
        try await runLoadGate(payloadSize: 1024 * 1024, startsStalled: false)
    }

    func testStalledRFC8441ReaderBoundsRSSThenReceivesExact128MiBFrame() async throws {
        let permit = await Phase3WebSocketLoadPermit.acquire()
        defer { withExtendedLifetime(permit) {} }
        try await runLoadGate(payloadSize: Phase3LargeWebSocketFrame.payloadSize, startsStalled: true)
    }

    private func runLoadGate(payloadSize: Int, startsStalled: Bool) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 3)
        let origin = try Phase3HTTP2WebSocketLoadOrigin(group: group, payloadSize: payloadSize)
        let mitmCA = try CertificateAuthority.generate().authority
        let sink = WebSocketRecordingSink()
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: Phase3LargeWebSocketFrame.captureLimit
        )
        let client = Phase3ProxyHTTP2WebSocketClient(group: group)
        var exchange: Phase3StalledWebSocketExchange?

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let baselineRSS = startsStalled ? try MachMemory.residentBytes() : nil
            exchange = try await runBlocking {
                try client.beginStalledWebSocketExchange(
                    proxyPort: proxyPort,
                    originHost: origin.hostname,
                    originPort: origin.localPort,
                    mitmCACertificatePEM: mitmCA.caCertificatePEM,
                    payloadSize: payloadSize,
                    startsStalled: startsStalled
                )
            }
            try await exerciseLoadGate(
                exchange: XCTUnwrap(exchange),
                origin: origin,
                sink: sink,
                baselineRSS: baselineRSS,
                payloadSize: payloadSize,
                startsStalled: startsStalled
            )

            await exchange?.shutdown()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            await exchange?.shutdown()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func exerciseLoadGate(
        exchange: Phase3StalledWebSocketExchange,
        origin: Phase3HTTP2WebSocketLoadOrigin,
        sink: WebSocketRecordingSink,
        baselineRSS: UInt64?,
        payloadSize: Int,
        startsStalled: Bool
    ) async throws {
        if !startsStalled {
            try await exerciseUnstalledLoadGate(
                exchange: exchange,
                origin: origin,
                sink: sink,
                payloadSize: payloadSize
            )
            return
        }
        let baselineRSS = try XCTUnwrap(baselineRSS)
        let stalledBytes = try await runBlocking { try origin.stalledBytes.wait() }
        let stalledRSS = try MachMemory.residentBytes()
        try assertStall(
            exchange: exchange,
            stalledBytes: stalledBytes,
            payloadSize: payloadSize,
            baselineRSS: baselineRSS,
            stalledRSS: stalledRSS,
            snapshot: sink.snapshot
        )
        let transferStartedAt = Date()
        let clientResult: Phase3StalledWebSocketResult
        let originSentBytes: Int
        do {
            try await exchange.resume()
            _ = try await exchange.resumedPayload.get()
            clientResult = try await exchange.completion.get()
            originSentBytes = try await runBlocking { try origin.sentBytes.wait() }
        } catch {
            writeFailureDiagnostic(exchange: exchange, origin: origin, sink: sink, error: error)
            throw error
        }
        let finalSnapshot = sink.snapshot
        try assertCompletion(
            clientResult: clientResult,
            originSentBytes: originSentBytes,
            payloadSize: payloadSize,
            snapshot: finalSnapshot
        )
        let measurement = Phase3WebSocketLoadMeasurement(
            stalledBytes: stalledBytes,
            rssGrowth: MachMemory.growth(from: baselineRSS, to: stalledRSS),
            transferSeconds: Date().timeIntervalSince(transferStartedAt),
            snapshot: finalSnapshot
        )
        FileHandle.standardError.write(Data((measurement.context + "\n").utf8))
    }

    private func exerciseUnstalledLoadGate(
        exchange: Phase3StalledWebSocketExchange,
        origin: Phase3HTTP2WebSocketLoadOrigin,
        sink: WebSocketRecordingSink,
        payloadSize: Int
    ) async throws {
        do {
            let clientResult = try await exchange.completion.get()
            let originSentBytes = try await runBlocking { try origin.sentBytes.wait() }
            try assertCompletion(
                clientResult: clientResult,
                originSentBytes: originSentBytes,
                payloadSize: payloadSize,
                snapshot: sink.snapshot
            )
        } catch {
            writeFailureDiagnostic(exchange: exchange, origin: origin, sink: sink, error: error)
            throw error
        }
    }

    private func assertStall(
        exchange: Phase3StalledWebSocketExchange,
        stalledBytes: Int,
        payloadSize: Int,
        baselineRSS: UInt64,
        stalledRSS: UInt64,
        snapshot: WebSocketRecordingSink.Snapshot
    ) throws {
        let rssGrowth = MachMemory.growth(from: baselineRSS, to: stalledRSS)
        XCTAssertGreaterThan(stalledBytes, 0)
        XCTAssertLessThan(stalledBytes, payloadSize)
        XCTAssertEqual(exchange.receivedPayloadBytes, 0)
        XCTAssertLessThan(
            rssGrowth,
            UInt64(64 * 1024 * 1024),
            "stalled RFC8441 RSS grew \(rssGrowth) bytes after \(stalledBytes) origin bytes"
        )
        XCTAssertTrue(snapshot.frames.isEmpty)
        XCTAssertTrue(snapshot.closes.isEmpty)
    }

    private func assertCompletion(
        clientResult: Phase3StalledWebSocketResult,
        originSentBytes: Int,
        payloadSize: Int,
        snapshot: WebSocketRecordingSink.Snapshot
    ) throws {
        XCTAssertEqual(clientResult.status, 200)
        XCTAssertEqual(clientResult.payloadBytes, payloadSize)
        XCTAssertEqual(clientResult.digest, Phase3LargeWebSocketFrame.expectedDigest(payloadSize: payloadSize))
        XCTAssertEqual(originSentBytes, payloadSize)
        let frame = try XCTUnwrap(snapshot.frames.only)
        let requestID = try XCTUnwrap(snapshot.requestHeads.only?.id)
        XCTAssertEqual(snapshot.responseHeads.only?.requestID, requestID)
        XCTAssertEqual(snapshot.opens.only?.connectionID, requestID)
        XCTAssertEqual(frame.connectionID, requestID)
        XCTAssertEqual(frame.direction, .serverToClient)
        XCTAssertEqual(frame.opcode, .binary)
        XCTAssertTrue(frame.fin)
        XCTAssertFalse(frame.compressed)
        XCTAssertEqual(frame.bytes.count, Phase3LargeWebSocketFrame.captureLimit)
        XCTAssertTrue(frame.bytes.allSatisfy { $0 == Phase3LargeWebSocketFrame.payloadByte })
        XCTAssertEqual(frame.byteCount, payloadSize)
        XCTAssertTrue(frame.truncated)
        XCTAssertEqual(snapshot.closes.map(\.connectionID), [requestID])
        XCTAssertEqual(snapshot.eventKinds.filter { $0 == .streamError }.count, 0)
    }

    private func writeFailureDiagnostic(
        exchange: Phase3StalledWebSocketExchange,
        origin: Phase3HTTP2WebSocketLoadOrigin,
        sink: WebSocketRecordingSink,
        error: Error
    ) {
        let originState = origin.diagnostic
        let snapshot = sink.snapshot
        let message = "RFC8441-FAILURE error=\(error) clientBytes=\(exchange.receivedPayloadBytes) "
            + "clientStreamActive=\(exchange.streamActive) clientConnectionActive=\(exchange.connectionActive) "
            + "originSent=\(originState.sentBytes) originRemaining=\(originState.remainingBytes) "
            + "originStreamActive=\(originState.streamActive) originConnectionActive=\(originState.connectionActive) "
            + "requestHeadersEndStream=\(String(describing: originState.requestHeadersEndStream)) "
            + "events=\(snapshot.eventKinds.count) opens=\(snapshot.opens.count) frames=\(snapshot.frames.count) "
            + "closes=\(snapshot.closes.count) errors="
            + "\(snapshot.eventKinds.filter { $0 == .streamError }.count) "
            + "eventKinds=\(String(describing: snapshot.eventKinds))\n"
        FileHandle.standardError.write(Data(message.utf8))
    }

    private func runBlocking<T: Sendable>(
        _ operation: @escaping @Sendable () throws -> T
    ) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

private extension Array {
    var only: Element? { count == 1 ? self[0] : nil }
}

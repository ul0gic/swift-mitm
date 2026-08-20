import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

@testable import SwiftMITM

final class CountingSink: CaptureEventSink, @unchecked Sendable {
    private let lock = NIOLock()
    private var requestHeads = 0
    private var responseHeads = 0
    private var responseBytes = 0
    private var responseEnds = 0
    private var errors = 0

    func receive(_ event: CaptureEvent) {
        lock.withLock {
            switch event {
            case .requestHead: requestHeads += 1
            case .responseHead: responseHeads += 1
            case .responseBodyChunk(_, _, let count): responseBytes += count
            case .responseEnd: responseEnds += 1
            case .streamError: errors += 1
            default: break
            }
        }
    }

    var capturedRequestHeads: Int { lock.withLock { requestHeads } }
    var capturedResponseHeads: Int { lock.withLock { responseHeads } }
    var capturedResponseBytes: Int { lock.withLock { responseBytes } }
    var capturedErrors: Int { lock.withLock { errors } }
}

private final class StreamingRSSSampler: @unchecked Sendable {
    private let peak: NIOLockedValueBox<UInt64>
    private let stopRequested = NIOLockedValueBox(false)
    private let samplingError = NIOLockedValueBox<MachMemoryError?>(nil)
    private let finished = DispatchGroup()

    init(baseline: UInt64) {
        peak = NIOLockedValueBox(baseline)
        finished.enter()
    }

    func start() {
        Thread {
            defer { self.finished.leave() }
            while !self.stopRequested.withLockedValue({ $0 }) {
                switch MachMemory.residentBytesResult() {
                case .success(let current):
                    self.peak.withLockedValue { $0 = max($0, current) }
                case .failure(let error):
                    self.samplingError.withLockedValue { $0 = error }
                    self.stopRequested.withLockedValue { $0 = true }
                }
                usleep(5000)
            }
        }.start()
    }

    func stop() throws -> UInt64 {
        stopRequested.withLockedValue { $0 = true }
        finished.wait()
        if let error = samplingError.withLockedValue({ $0 }) {
            throw error
        }
        return peak.withLockedValue { $0 }
    }
}

final class StreamingBackpressureTests: XCTestCase {
    func testBackpressureBoundsMemoryWhileStalledThenDeliversInFull() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let bodySize = 256 * 1024 * 1024
        let origin = TestOriginServer(group: group, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }
        let originPort = origin.localPort

        let sink = CountingSink()
        let proxy = HTTP2ProxyBridge(
            group: group,
            upstream: .init(host: "127.0.0.1", port: originPort),
            sink: sink
        )
        let proxyChannel = try proxy.start().wait()
        defer { try? proxyChannel.close().wait() }
        let proxyPort = try XCTUnwrap(proxyChannel.localAddress?.port)

        let client = TestHTTP2Client(group: group)
        defer { client.shutdown() }
        let authority = "127.0.0.1:\(originPort)"
        try client.connectAndRequest(
            host: "127.0.0.1",
            port: proxyPort,
            authority: authority,
            startPaused: true
        )

        Thread.sleep(forTimeInterval: 0.3)
        let baseline = try MachMemory.residentBytes()
        var peak = baseline
        let deadline = Date().addingTimeInterval(1.5)
        while Date() < deadline {
            peak = max(peak, try MachMemory.residentBytes())
            Thread.sleep(forTimeInterval: 0.02)
        }
        let stalledDelta = MachMemory.growth(from: baseline, to: peak)
        XCTAssertLessThan(
            stalledDelta,
            UInt64(32 * 1024 * 1024),
            "RSS grew \(stalledDelta) bytes while the consumer was stalled on a \(bodySize)-byte body — "
                + "backpressure is not holding across the two proxy legs"
        )

        client.resume()
        let received = try client.completion.wait()
        XCTAssertEqual(received, bodySize, "stream did not deliver in full after resume (possible deadlock)")
        XCTAssertEqual(sink.capturedResponseBytes, bodySize, "capture tap byte total disagrees with client")
        XCTAssertGreaterThan(sink.capturedRequestHeads, 0)
        XCTAssertEqual(sink.capturedErrors, 0)
    }

    func testFullSpeedTransferStaysBoundedAndRecordsThroughput() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let bodySize = 256 * 1024 * 1024
        let origin = TestOriginServer(group: group, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }
        let originPort = origin.localPort

        let sink = CountingSink()
        let proxy = HTTP2ProxyBridge(
            group: group,
            upstream: .init(host: "127.0.0.1", port: originPort),
            sink: sink
        )
        let proxyChannel = try proxy.start().wait()
        defer { try? proxyChannel.close().wait() }
        let proxyPort = try XCTUnwrap(proxyChannel.localAddress?.port)

        let baseline = try MachMemory.residentBytes()
        let sampler = StreamingRSSSampler(baseline: baseline)
        sampler.start()
        defer { _ = try? sampler.stop() }

        let client = TestHTTP2Client(group: group)
        defer { client.shutdown() }
        let authority = "127.0.0.1:\(originPort)"
        let start = Date()
        try client.connectAndRequest(host: "127.0.0.1", port: proxyPort, authority: authority)
        let received = try client.completion.wait()
        let elapsed = Date().timeIntervalSince(start)
        let peakDelta = MachMemory.growth(from: baseline, to: try sampler.stop())
        let throughputMBps = Double(bodySize) / 1_000_000 / elapsed
        let measurement =
            "STREAMING-MEASURE full-speed: body=\(bodySize / (1024 * 1024))MiB "
            + "elapsed=\(String(format: "%.3f", elapsed))s "
            + "throughput=\(String(format: "%.1f", throughputMBps))MB/s "
            + "peakRSSDelta=\(peakDelta / (1024 * 1024))MiB\n"
        FileHandle.standardError.write(Data(measurement.utf8))

        XCTAssertEqual(received, bodySize)
        XCTAssertLessThan(
            peakDelta,
            UInt64(96 * 1024 * 1024),
            "peak RSS delta \(peakDelta) too high for a streamed \(bodySize)-byte body"
        )
    }
}

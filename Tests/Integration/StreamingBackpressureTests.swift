import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

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
    func testBackpressureBoundsMemoryWhileStalledThenDeliversInFull() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let bodySize = 256 * 1024 * 1024
        let origin = try TLSOriginServer(group: group, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }
        let originPort = origin.localPort

        let sink = CountingSink()
        let authority = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )
        let proxyPort = try await proxy.start(port: 0)

        let fetch: ProxyStalledFetch
        do {
            fetch = try ProxyTestClient(group: group).beginStalledFetch(
                proxyPort: proxyPort,
                originHost: origin.hostname,
                originPort: originPort,
                mitmCACertificatePEM: authority.caCertificatePEM,
                alpn: "h2",
                timeout: .seconds(120)
            )
        } catch {
            try? await proxy.stop()
            throw error
        }
        do {
            try await Task.sleep(for: .milliseconds(300))
            let baseline = try MachMemory.residentBytes()
            var peak = baseline
            let deadline = Date().addingTimeInterval(1.5)
            while Date() < deadline {
                peak = max(peak, try MachMemory.residentBytes())
                try await Task.sleep(for: .milliseconds(20))
            }
            let stalledDelta = MachMemory.growth(from: baseline, to: peak)
            XCTAssertLessThan(
                stalledDelta,
                UInt64(32 * 1024 * 1024),
                "RSS grew \(stalledDelta) bytes while the consumer was stalled on a \(bodySize)-byte body — "
                    + "backpressure is not holding across the two proxy legs"
            )

            try await fetch.resume()
            let received = try await fetch.completion.get()
            XCTAssertEqual(received, bodySize, "stream did not deliver in full after resume (possible deadlock)")
            XCTAssertEqual(sink.capturedResponseBytes, bodySize, "capture tap byte total disagrees with client")
            XCTAssertGreaterThan(sink.capturedRequestHeads, 0)
            XCTAssertEqual(sink.capturedErrors, 0)
            await fetch.shutdown()
            try await proxy.stop()
        } catch {
            await fetch.shutdown()
            try? await proxy.stop()
            throw error
        }
    }

    func testFullSpeedTransferStaysBoundedAndRecordsThroughput() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let bodySize = 256 * 1024 * 1024
        let origin = try TLSOriginServer(group: group, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }
        let originPort = origin.localPort

        let sink = CountingSink()
        let authority = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )
        let proxyPort = try await proxy.start(port: 0)

        let baseline = try MachMemory.residentBytes()
        let sampler = StreamingRSSSampler(baseline: baseline)
        sampler.start()
        defer { _ = try? sampler.stop() }

        let start = Date()
        let received: Int
        do {
            received = try ProxyTestClient(group: group).fetch(
                proxyPort: proxyPort,
                originHost: origin.hostname,
                originPort: originPort,
                mitmCACertificatePEM: authority.caCertificatePEM,
                alpn: "h2"
            )
            try await proxy.stop()
        } catch {
            try? await proxy.stop()
            throw error
        }
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

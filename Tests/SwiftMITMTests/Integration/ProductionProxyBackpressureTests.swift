import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

@testable import SwiftMITM

private let productionLoadSemaphore = DispatchSemaphore(value: 1)

private final class ProductionLoadPermit: @unchecked Sendable {
    static func acquire() async -> ProductionLoadPermit {
        await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                productionLoadSemaphore.wait()
                continuation.resume(returning: ProductionLoadPermit())
            }
        }
    }

    deinit {
        productionLoadSemaphore.signal()
    }
}

private struct ProductionCaptureSnapshot: Sendable {
    let capturedBytes: Int
    let observedBytes: Int
    let responseHeads: Int
    let responseEnds: Int
    let streamErrors: Int
    let truncated: Bool
}

private struct ProductionStallMeasurement: Sendable {
    let alpn: String
    let bodySize: Int
    let captureLimit: Int
    let baselineRSS: UInt64
    let peakRSS: UInt64
    let stalledClientBytes: Int
    let stalledObservedBytes: Int
    let elapsed: TimeInterval

    var rssGrowth: Int64 { Int64(peakRSS) - Int64(baselineRSS) }

    var context: String {
        let mibibyte = 1024 * 1024
        return "PRODUCTION-BACKPRESSURE protocol=\(alpn) body=\(bodySize / mibibyte)MiB "
            + "captureLimit=\(captureLimit / 1024)KiB baselineRSS=\(baselineRSS / UInt64(mibibyte))MiB "
            + "peakRSS=\(peakRSS / UInt64(mibibyte))MiB stalledRSSDelta=\(rssGrowth / Int64(mibibyte))MiB "
            + "stalledClientBytes=\(stalledClientBytes) stalledObservedBytes=\(stalledObservedBytes) "
            + "stallSeconds=\(String(format: "%.3f", elapsed)) cores=\(System.coreCount)"
    }
}

private final class ProductionLoadSink: CaptureEventSink, @unchecked Sendable {
    private let lock = NIOLock()
    private var capturedBytes = 0
    private var observedBytes = 0
    private var responseHeads = 0
    private var responseEnds = 0
    private var streamErrors = 0
    private var truncated = false

    func receive(_ event: CaptureEvent) {
        lock.withLock {
            switch event {
            case .responseHead:
                responseHeads += 1
            case let .responseBodyChunk(_, bytes, byteCount):
                capturedBytes += bytes.count
                observedBytes += byteCount
            case .responseEnd(_, let wasTruncated):
                responseEnds += 1
                truncated = truncated || wasTruncated
            case .streamError:
                streamErrors += 1
            default:
                break
            }
        }
    }

    var snapshot: ProductionCaptureSnapshot {
        lock.withLock {
            ProductionCaptureSnapshot(
                capturedBytes: capturedBytes,
                observedBytes: observedBytes,
                responseHeads: responseHeads,
                responseEnds: responseEnds,
                streamErrors: streamErrors,
                truncated: truncated
            )
        }
    }
}

private struct ProductionLoadEnvironment {
    let bodySize = 128 * 1024 * 1024
    let captureLimit = 64 * 1024
    let maximumRSSGrowth = Int64(64 * 1024 * 1024)
    let traffic: MultiThreadedEventLoopGroup
    let origin: TLSOriginServer
    let mitmCA: CertificateAuthority
    let sink: ProductionLoadSink
    let proxy: ProxyServer

    init(alpn: String) throws {
        traffic = MultiThreadedEventLoopGroup(numberOfThreads: 3)
        origin = try TLSOriginServer(group: traffic, bodySize: bodySize, applicationProtocols: [alpn])
        try origin.start()
        mitmCA = try CertificateAuthority.generate().authority
        sink = ProductionLoadSink()
        proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: captureLimit
        )
    }

    func shutDown() async {
        try? await proxy.stop()
        origin.stop()
        try? await traffic.shutdownGracefully()
    }
}

final class ProductionProxyBackpressureTests: XCTestCase {
    func testHTTP1ProductionPathBoundsMemoryWhileStalledThenDeliversExactly() async throws {
        try await assertProductionPath(alpn: "http/1.1")
    }

    func testHTTP2ProductionPathBoundsMemoryWhileStalledThenDeliversExactly() async throws {
        try await assertProductionPath(alpn: "h2")
    }

    private func assertProductionPath(alpn: String) async throws {
        let permit = await ProductionLoadPermit.acquire()
        defer { withExtendedLifetime(permit) {} }
        let environment = try ProductionLoadEnvironment(alpn: alpn)
        var stalledFetch: ProxyStalledFetch?

        do {
            stalledFetch = try await executeStalledTransfer(alpn: alpn, environment: environment)
            if let stalledFetch { await stalledFetch.shutdown() }
            await environment.shutDown()
        } catch {
            if let stalledFetch { await stalledFetch.shutdown() }
            await environment.shutDown()
            throw error
        }
    }

    private func executeStalledTransfer(
        alpn: String,
        environment: ProductionLoadEnvironment
    ) async throws -> ProxyStalledFetch {
        let proxyPort = try await environment.proxy.start(port: 0)
        let baselineRSS = MachMemory.residentBytes()
        let startedAt = Date()
        let originHost = environment.origin.hostname
        let originPort = environment.origin.localPort
        let mitmCertificate = environment.mitmCA.caCertificatePEM
        let traffic = environment.traffic
        let fetch = try await runBlocking {
            try ProxyTestClient(group: traffic).beginStalledFetch(
                proxyPort: proxyPort,
                originHost: originHost,
                originPort: originPort,
                mitmCACertificatePEM: mitmCertificate,
                alpn: alpn,
                timeout: .seconds(120)
            )
        }
        do {
            var peakRSS = max(baselineRSS, MachMemory.residentBytes())
            let stallDeadline = Date().addingTimeInterval(1.5)
            while Date() < stallDeadline {
                peakRSS = max(peakRSS, MachMemory.residentBytes())
                try await Task.sleep(for: .milliseconds(20))
            }

            let stalledCapture = environment.sink.snapshot
            let measurement = ProductionStallMeasurement(
                alpn: alpn,
                bodySize: environment.bodySize,
                captureLimit: environment.captureLimit,
                baselineRSS: baselineRSS,
                peakRSS: peakRSS,
                stalledClientBytes: fetch.receivedBytes,
                stalledObservedBytes: stalledCapture.observedBytes,
                elapsed: Date().timeIntervalSince(startedAt)
            )
            FileHandle.standardError.write(Data((measurement.context + "\n").utf8))
            assertStall(measurement, capture: stalledCapture, maximumRSSGrowth: environment.maximumRSSGrowth)

            try await fetch.resume()
            let received = try await fetch.completion.get()
            assertCompletion(received: received, capture: environment.sink.snapshot, measurement: measurement)
            return fetch
        } catch {
            await fetch.shutdown()
            throw error
        }
    }

    private func assertStall(
        _ measurement: ProductionStallMeasurement,
        capture: ProductionCaptureSnapshot,
        maximumRSSGrowth: Int64
    ) {
        XCTAssertEqual(
            measurement.stalledClientBytes,
            0,
            "downstream consumed bytes before resume; \(measurement.context)"
        )
        XCTAssertLessThan(
            measurement.rssGrowth,
            maximumRSSGrowth,
            "stalled production path exceeded the RSS gate; \(measurement.context)"
        )
        XCTAssertLessThanOrEqual(
            capture.capturedBytes,
            measurement.captureLimit,
            "capture exceeded its configured limit while stalled; \(measurement.context)"
        )
    }

    private func assertCompletion(
        received: Int,
        capture: ProductionCaptureSnapshot,
        measurement: ProductionStallMeasurement
    ) {
        XCTAssertEqual(received, measurement.bodySize, "forwarded byte count was not exact; \(measurement.context)")
        XCTAssertEqual(
            capture.observedBytes,
            measurement.bodySize,
            "capture observation was not exact; \(measurement.context)"
        )
        XCTAssertEqual(
            capture.capturedBytes,
            measurement.captureLimit,
            "capture was not bounded exactly; \(measurement.context)"
        )
        XCTAssertEqual(capture.responseHeads, 1, "unexpected response head count; \(measurement.context)")
        XCTAssertEqual(capture.responseEnds, 1, "unexpected response end count; \(measurement.context)")
        XCTAssertEqual(capture.streamErrors, 0, "proxy emitted a stream error; \(measurement.context)")
        XCTAssertTrue(capture.truncated, "bounded capture did not report truncation; \(measurement.context)")
    }

    private func runBlocking<T: Sendable>(_ operation: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

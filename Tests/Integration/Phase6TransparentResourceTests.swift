import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

private struct Phase6TransparentSetup {
    let adapter: Phase6IngressAdapter
    let fetch: Phase6TransparentStalledFetch
    let baselineRSS: UInt64
}

final class Phase6TransparentResourceTests: XCTestCase {
    private static let bodySize = 128 * 1024 * 1024
    private static let bodyByte: UInt8 = 0x41

    func testTrustedTransparentHTTP11StallBoundsRSSAndDeliversExact128MiB() async throws {
        let permit = await Phase6ResourcePermit.acquire()
        defer { withExtendedLifetime(permit) {} }
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 3)
        let origin = try TLSOriginServer(
            group: group,
            bodySize: Self.bodySize,
            applicationProtocols: ["http/1.1"]
        )
        let authority = try CertificateAuthority.generate().authority
        let sink = Phase6ResourceSink()
        let proxy = try makeProxy(group: group, origin: origin, authority: authority, sink: sink)
        var adapter: Phase6IngressAdapter?
        var fetch: Phase6TransparentStalledFetch?

        do {
            try origin.start()
            let setup = try await startTransfer(
                proxy: proxy,
                origin: origin,
                authority: authority,
                group: group
            )
            adapter = setup.adapter
            fetch = setup.fetch
            let stalledCapture = try sink.waitForHTTPProgress(
                minimumObservedBytes: Phase6ResourceStream.captureLimit
            )
            let stalledRSS = try MachMemory.residentBytes()
            try assertStall(
                fetch: XCTUnwrap(fetch),
                baselineRSS: setup.baselineRSS,
                stalledRSS: stalledRSS,
                capture: stalledCapture
            )

            let transferStartedAt = Date()
            try await fetch?.resume()
            let result = try await XCTUnwrap(fetch).result.get()
            let snapshot = try sink.waitForHTTPCompletion()
            try assertCompletion(result: result, originPort: origin.localPort, snapshot: snapshot)
            writeMeasurement(
                baselineRSS: setup.baselineRSS,
                stalledRSS: stalledRSS,
                stalledCapture: stalledCapture,
                elapsed: Date().timeIntervalSince(transferStartedAt),
                snapshot: snapshot
            )
            await cleanup(fetch: fetch, adapter: adapter, proxy: proxy, origin: origin, group: group)
        } catch {
            await cleanup(fetch: fetch, adapter: adapter, proxy: proxy, origin: origin, group: group)
            throw error
        }
    }

    private func startTransfer(
        proxy: ProxyServer,
        origin: TLSOriginServer,
        authority: CertificateAuthority,
        group: EventLoopGroup
    ) async throws -> Phase6TransparentSetup {
        let proxyPort = try await proxy.start(port: 0)
        let adapter = Phase6IngressAdapter(
            group: group,
            proxyPort: proxyPort,
            destinationPort: origin.localPort
        )
        do {
            try adapter.start()
            let baselineRSS = try MachMemory.residentBytes()
            let adapterPort = adapter.localPort
            let originHost = origin.hostname
            let originPort = origin.localPort
            let authorityPEM = authority.caCertificatePEM
            let fetch = try await Self.runBlocking {
                try ProxyTestClient(group: group).beginPhase6TransparentStalledFetch(
                    adapterPort: adapterPort,
                    originHost: originHost,
                    originPort: originPort,
                    mitmCACertificatePEM: authorityPEM,
                    bodySize: Self.bodySize
                )
            }
            return .init(adapter: adapter, fetch: fetch, baselineRSS: baselineRSS)
        } catch {
            adapter.stop()
            throw error
        }
    }

    private func makeProxy(
        group: EventLoopGroup,
        origin: TLSOriginServer,
        authority: CertificateAuthority,
        sink: Phase6ResourceSink
    ) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        return ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            captureBodyLimit: Phase6ResourceStream.captureLimit
        )
    }

    private func assertStall(
        fetch: Phase6TransparentStalledFetch,
        baselineRSS: UInt64,
        stalledRSS: UInt64,
        capture: Phase6ResourceSink.Snapshot
    ) throws {
        let growth = MachMemory.growth(from: baselineRSS, to: stalledRSS)
        XCTAssertEqual(fetch.receivedBytes, 0)
        XCTAssertGreaterThanOrEqual(capture.responseObservedBytes, Phase6ResourceStream.captureLimit)
        XCTAssertEqual(capture.responseRetainedBytes.count, Phase6ResourceStream.captureLimit)
        XCTAssertLessThan(capture.responseObservedBytes, Self.bodySize)
        XCTAssertLessThan(growth, Phase6ResourceStream.maximumRSSGrowth)
    }

    private func assertCompletion(
        result: Phase6DigestResult,
        originPort: Int,
        snapshot: Phase6ResourceSink.Snapshot
    ) throws {
        let expectedDigest = Phase6ResourceStream.digest(repeating: Self.bodyByte, count: Self.bodySize)
        XCTAssertEqual(result, .init(byteCount: Self.bodySize, digest: expectedDigest))
        XCTAssertEqual(snapshot.responseHeads, 1)
        XCTAssertEqual(snapshot.responseObservedBytes, Self.bodySize)
        XCTAssertEqual(snapshot.responseRetainedBytes, Array(repeating: Self.bodyByte, count: 64 * 1024))
        XCTAssertEqual(snapshot.responseEnds.count, 1)
        XCTAssertEqual(snapshot.responseEnds.first?.truncated, true)
        XCTAssertEqual(snapshot.streamErrors, 0)
        let request = try XCTUnwrap(snapshot.requestHeads.first)
        XCTAssertEqual(snapshot.responseEnds.first?.requestID, request.id)
        XCTAssertEqual(request.target?.destination.address, "127.0.0.1")
        XCTAssertEqual(request.target?.destination.port, originPort)
        XCTAssertEqual(request.target?.ingressProvenance, .trustedProxyV2)
    }

    private func writeMeasurement(
        baselineRSS: UInt64,
        stalledRSS: UInt64,
        stalledCapture: Phase6ResourceSink.Snapshot,
        elapsed: TimeInterval,
        snapshot: Phase6ResourceSink.Snapshot
    ) {
        let growth = MachMemory.growth(from: baselineRSS, to: stalledRSS)
        let mebibyte = 1024 * 1024
        let throughput = Double(Self.bodySize) / Double(mebibyte) / elapsed
        let message = "PHASE6-TRANSPARENT-RESOURCE body=\(Self.bodySize / mebibyte)MiB "
            + "stalledObserved=\(stalledCapture.responseObservedBytes) stalledClientBytes=0 "
            + "rssDelta=\(growth / UInt64(mebibyte))MiB "
            + "retained=\(snapshot.responseRetainedBytes.count) observed=\(snapshot.responseObservedBytes) "
            + "seconds=\(String(format: "%.3f", elapsed)) "
            + "throughputMiBps=\(String(format: "%.3f", throughput))\n"
        FileHandle.standardError.write(Data(message.utf8))
    }

    private func cleanup(
        fetch: Phase6TransparentStalledFetch?,
        adapter: Phase6IngressAdapter?,
        proxy: ProxyServer,
        origin: TLSOriginServer,
        group: EventLoopGroup
    ) async {
        await fetch?.shutdown()
        adapter?.stop()
        try? await proxy.stop()
        origin.stop()
        try? await group.shutdownGracefully()
    }

    private static func runBlocking<T: Sendable>(
        _ operation: @escaping @Sendable () throws -> T
    ) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

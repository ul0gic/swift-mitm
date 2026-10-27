import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

import SwiftMITM
import SwiftMITMInspection

func stage2H2Stream(_ multiplexer: NIOHTTP2Handler.StreamMultiplexer) throws -> Release022H2Stream {
    let stream = Release022H2Stream()
    _ = try phase4BoundedWait(multiplexer.createStreamChannel { channel in
        channel.eventLoop.makeCompletedFuture { try channel.pipeline.syncOperations.addHandler(stream) }
    })
    return stream
}

func stage2H2Bytes(_ frames: [HTTP2Frame.FramePayload]) -> [UInt8] {
    frames.flatMap { payload -> [UInt8] in
        if case let .data(data) = payload, case let .byteBuffer(buffer) = data.data {
            Array(buffer.readableBytesView)
        } else {
            []
        }
    }
}

func stage2H2Ended(_ frames: [HTTP2Frame.FramePayload]) -> Bool {
    frames.contains { payload in
        switch payload {
        case let .headers(headers): headers.endStream
        case let .data(data): data.endStream
        default: false
        }
    }
}

func stage2H2Headers(port: Int, path: String, scheme: String = "https", host: String = "localhost") -> HPACKHeaders {
    HPACKHeaders([
        (":method", "POST"),
        (":scheme", scheme),
        (":authority", "\(host):\(port)"),
        (":path", path),
        ("content-type", "application/grpc"),
        ("te", "trailers")
    ])
}

final class Stage2HTTP2AcceptanceTests: XCTestCase {
    func testNativeGRPCUsesPinnedSchemaWhileRegistryRejectsAndReplacesSnapshots() async throws {
        let schema = try await Stage2SchemaProbe()
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Release022HTTP2Origin(group: group)
        let root = try CertificateAuthority.generate().authority
        let raw = Release022Events()
        let events = Stage2InspectionEvents()
        let inspector = try HTTPInspector(
            configuration: InspectionConfiguration(budget: schema.budget, schema: schema.original),
            sink: events
        )
        let proxy = ProxyServer(
            certificateAuthority: root,
            sink: raw,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.authority.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 0,
            observationSink: inspector
        )
        var channel: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let client = ProxyTestClient(group: group)
            let tunnel = try client.openTunnel(proxyPort: port, originHost: "localhost", originPort: origin.port)
            channel = tunnel
            let multiplexer = try installedMultiplexer(client: client, tunnel: tunnel, root: root)
            let downstream = try beginSchemaStream(multiplexer, originPort: origin.port)
            let upstream = try origin.stream(0)
            try await schema.exercise(upstream: upstream, downstream: downstream, events: events, raw: raw)
            try await tunnel.close().get()
            try await proxy.stop()
            origin.stop()
            let shutdown = await inspector.shutdown()
            XCTAssertTrue(shutdown.drained)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do {
                if let channel, channel.isActive {
                    try await channel.close().get()
                }; try await proxy.stop()
            } catch { XCTFail("H2 proxy teardown failed: \(error)") }
            origin.stop()
            let shutdown = await inspector.shutdown()
            XCTAssertTrue(shutdown.drained)
            do { try await group.shutdownGracefully() } catch { XCTFail("H2 group teardown failed: \(error)") }
            throw failure
        }
    }

    func testPausedHTTP2StreamDoesNotBlockThirtyOneSiblingsAndCancelsOnce() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 13, applicationProtocols: ["h2"])
        let root = try CertificateAuthority.generate().authority
        let controls = Stage2ControlEvents()
        let runtime = try pausedStreamRuntime()
        let proxy = ProxyServer(
            certificateAuthority: root,
            sink: Release022Events(),
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            controlRuntime: runtime,
            controlEventSink: controls
        )
        var channel: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let client = ProxyTestClient(group: group)
            let tunnel = try client.openTunnel(proxyPort: port, originHost: "localhost", originPort: origin.localPort)
            channel = tunnel
            let multiplexer = try installedMultiplexer(client: client, tunnel: tunnel, root: root)
            try await assertPausedStreamIsolation(
                multiplexer: multiplexer,
                originPort: origin.localPort,
                controls: controls,
                runtime: runtime
            )
            try await tunnel.close().get()
            try await proxy.stop()
            origin.stop()
            let usage = await runtime.resourceUsage()
            XCTAssertEqual(usage.pausedExchanges, 0)
            XCTAssertEqual(usage.retainedBytes, 0)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do {
                if let channel, channel.isActive {
                    try await channel.close().get()
                }; try await proxy.stop()
            } catch { XCTFail("H2 control teardown failed: \(error)") }
            origin.stop()
            do { try await group.shutdownGracefully() } catch { XCTFail("H2 control group teardown failed: \(error)") }
            throw failure
        }
    }

    private func assertPausedStreamIsolation(
        multiplexer: NIOHTTP2Handler.StreamMultiplexer,
        originPort: Int,
        controls: Stage2ControlEvents,
        runtime: HTTPControlRuntime
    ) async throws {
        let held = try stage2H2Stream(multiplexer)
        try held.write(.headers(.init(headers: stage2H2Headers(port: originPort, path: "/hold"), endStream: true)))
        let handle = try await controls.breakpoint()
        var siblings: [Release022H2Stream] = []
        for index in 0..<31 {
            let stream = try stage2H2Stream(multiplexer)
            try stream.write(.headers(.init(
                headers: stage2H2Headers(port: originPort, path: "/sibling/\(index)"),
                endStream: true
            )))
            siblings.append(stream)
        }
        for sibling in siblings {
            try sibling.wait(until: stage2H2Ended)
            XCTAssertEqual(stage2H2Bytes(sibling.frames).count, 13)
        }
        XCTAssertTrue(held.frames.isEmpty)
        try held.write(.rstStream(.cancel))
        try await stage2AwaitAsync { await runtime.resourceUsage().pausedExchanges == 0 }
        let resolution = await handle.resolve(.forwardOriginal)
        if case .alreadyCompleted = resolution {} else {
            XCTFail("Cancelled H2 stream accepted late breakpoint response")
        }
        let effectiveRequests = controls.events.filter { event in
            if case let .effectiveHead(_, .request, headers, _) = event {
                headers.contains { $0.name == "x-proof" && $0.value == "h2" }
            } else {
                false
            }
        }
        XCTAssertEqual(effectiveRequests.count, 31)
    }

    private func installedMultiplexer(
        client: ProxyTestClient,
        tunnel: Channel,
        root: CertificateAuthority
    ) throws -> NIOHTTP2Handler.StreamMultiplexer {
        let tls = try client.installTLS(
            on: tunnel,
            serverHostname: "localhost",
            mitmCACertificatePEM: root.caCertificatePEM,
            applicationProtocols: ["h2"],
            configuresHTTP2: true
        )
        try phase4BoundedWait(tls.installed)
        XCTAssertEqual(try phase4BoundedWait(tls.handshake), "h2")
        return try XCTUnwrap(try phase4BoundedWait(try XCTUnwrap(tls.multiplexer)))
    }

    private func pausedStreamRuntime() throws -> HTTPControlRuntime {
        HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "hold-h2",
                match: HTTPRuleMatch(phase: .request, path: "/hold"),
                action: .breakpoint
            ),
            HTTPRule(
                id: "edit-h2",
                match: HTTPRuleMatch(phase: .request),
                action: .editHeaders(remove: [], append: [HTTPHeaderField(name: "x-proof", value: "h2")])
            )
        ], limits: HTTPControlLimits()))
    }

    private func beginSchemaStream(
        _ multiplexer: NIOHTTP2Handler.StreamMultiplexer,
        originPort: Int
    ) throws -> Release022H2Stream {
        let downstream = try stage2H2Stream(multiplexer)
        try downstream.write(.headers(.init(
            headers: stage2H2Headers(port: originPort, path: "/stage2.Probe/Call"),
            endStream: true
        )))
        return downstream
    }
}

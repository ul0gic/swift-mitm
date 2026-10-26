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
    frames.flatMap { payload in
        if case .data(let data) = payload, case .byteBuffer(let buffer) = data.data { Array(buffer.readableBytesView) } else { [] }
    }
}

func stage2H2Ended(_ frames: [HTTP2Frame.FramePayload]) -> Bool {
    frames.contains { payload in
        switch payload {
        case .headers(let headers): headers.endStream
        case .data(let data): data.endStream
        default: false
        }
    }
}

func stage2H2Headers(port: Int, path: String, scheme: String = "https", host: String = "localhost") -> HPACKHeaders {
    HPACKHeaders([
        (":method", "POST"), (":scheme", scheme), (":authority", "\(host):\(port)"), (":path", path),
        ("content-type", "application/grpc"), ("te", "trailers")
    ])
}

final class Stage2HTTP2AcceptanceTests: XCTestCase {
    func testNativeGRPCUsesPinnedSchemaWhileRegistryRejectsAndReplacesSnapshots() async throws {
        let originalPath = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_DESCRIPTOR_PATH"])
        let renamedPath = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_RENAMED_DESCRIPTOR_PATH"])
        let originalDescriptor = [UInt8](try Data(contentsOf: URL(fileURLWithPath: originalPath)))
        let renamedDescriptor = [UInt8](try Data(contentsOf: URL(fileURLWithPath: renamedPath)))
        let maximumDescriptorBytes = max(originalDescriptor.count, renamedDescriptor.count)
        let budget = try InspectionMemoryBudget()
        let registry = ProtobufSchemaRegistry(budget: budget, limits: try ProtobufSchemaLimits(
            descriptorBytes: maximumDescriptorBytes, files: 1, fields: 1
        ))
        let original = try await registry.load(descriptorBytes: originalDescriptor)
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Release022HTTP2Origin(group: group)
        let root = try CertificateAuthority.generate().authority
        let raw = Release022Events()
        let events = Stage2InspectionEvents()
        let inspector = try HTTPInspector(configuration: InspectionConfiguration(budget: budget, schema: original), sink: events)
        let proxy = ProxyServer(
            certificateAuthority: root, sink: raw, group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.authority.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true), captureBodyLimit: 0, observationSink: inspector
        )
        var channel: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let client = ProxyTestClient(group: group)
            let tunnel = try client.openTunnel(proxyPort: port, originHost: "localhost", originPort: origin.port)
            channel = tunnel
            let tls = try client.installTLS(on: tunnel, serverHostname: "localhost", mitmCACertificatePEM: root.caCertificatePEM,
                                            applicationProtocols: ["h2"], configuresHTTP2: true)
            try phase4BoundedWait(tls.installed)
            XCTAssertEqual(try phase4BoundedWait(tls.handshake), "h2")
            let multiplexer = try XCTUnwrap(try phase4BoundedWait(try XCTUnwrap(tls.multiplexer)))
            let downstream = try stage2H2Stream(multiplexer)
            try downstream.write(.headers(.init(headers: stage2H2Headers(port: origin.port, path: "/stage2.Probe/Call"), endStream: true)))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([(":status", "200"), ("content-type", "application/grpc"), ("grpc-encoding", "gzip")]))))
            let envelope: [UInt8] = [0, 0, 0, 0, 5, 8, 42, 0x98, 6, 7]
            for slice in [Array(envelope.prefix(2)), Array(envelope[2..<7]), Array(envelope.dropFirst(7))] {
                try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: slice)))))
            }
            let first = try await events.wait { events in
                events.contains { if case .protobuf = $0.payload { true } else { false } }
            }
            let decoded = try XCTUnwrap(first.compactMap { event -> (String, String, [ProtobufFieldOccurrence])? in
                if case .protobuf(let name, let digest, let fields) = event.payload { (name, digest, fields) } else { nil }
            }.first)
            XCTAssertEqual(decoded.0, "stage2.Envelope")
            XCTAssertEqual(decoded.1, original.digest)
            XCTAssertEqual(decoded.2.map(\.number), [1, 99])
            XCTAssertEqual(decoded.2.first?.name, "value")
            if let field = decoded.2.first, case .signed(let value) = field.value { XCTAssertEqual(value, 42) }
            else { XCTFail("Schema scalar value was not interpreted as int32") }
            if let field = decoded.2.last, case .unknown(let wireType, let bytes) = field.value {
                XCTAssertEqual(wireType, 0)
                XCTAssertEqual(bytes, [0x98, 6, 7])
            } else { XCTFail("Unknown wire field was lost") }
            XCTAssertTrue(release022ResponseEnds(raw.events).isEmpty)
            do {
                _ = try await registry.load(descriptorBytes: [0x0A, 0xFF])
                XCTFail("Malformed descriptor replaced active schema")
            } catch {
                let selected = await registry.current
                XCTAssertEqual(selected?.digest, original.digest)
            }
            do {
                _ = try await registry.load(descriptorBytes: [UInt8](repeating: 0, count: maximumDescriptorBytes + 1))
                XCTFail("Descriptor limit plus one was accepted")
            } catch ProtobufInspectionError.limitExceeded {
                let selected = await registry.current
                XCTAssertEqual(selected?.digest, original.digest)
            } catch { throw error }
            let replacement = try await registry.load(descriptorBytes: renamedDescriptor)
            XCTAssertNotEqual(replacement.digest, original.digest)
            XCTAssertEqual(try replacement.decode([8, 42], messageName: "stage2.Envelope").first?.name, "changed")
            try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: envelope)))))
            let vectorPath = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_COMPRESSION_VECTORS"])
            let compressedFrames = [UInt8](try Data(contentsOf: URL(fileURLWithPath: vectorPath).appendingPathComponent("grpc-gzip.bin")))
            try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: compressedFrames)))))
            try upstream.write(.headers(.init(headers: HPACKHeaders([("grpc-status", "0")]), endStream: true)))
            try downstream.wait(until: stage2H2Ended)
            let completed = try await events.wait { events in
                events.contains { if case .grpc(.status(let status)) = $0.payload { status.code == 0 } else { false } }
            }
            let names = completed.compactMap { event -> String? in
                if case .protobuf(_, _, let fields) = event.payload { fields.first?.name } else { nil }
            }
            XCTAssertEqual(names, ["value", "value", "value", "value"])
            let messages = completed.compactMap { event -> GRPCMessage? in
                if case .grpc(.message(let message)) = event.payload, event.direction == .response { message } else { nil }
            }
            XCTAssertEqual(messages.map(\.compressed), [false, false, true, true])
            XCTAssertEqual(messages.suffix(2).map(\.bytes), [[8, 42], [8, 42]])
            XCTAssertEqual(stage2H2Bytes(downstream.frames), envelope + envelope + compressedFrames)
            XCTAssertTrue(release022ResponseBodyBytes(raw.events).isEmpty)
            try await tunnel.close().get()
            try await proxy.stop()
            origin.stop()
            let shutdown = await inspector.shutdown()
            XCTAssertTrue(shutdown.drained)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do { if let channel, channel.isActive { try await channel.close().get() }; try await proxy.stop() }
            catch { XCTFail("H2 proxy teardown failed: \(error)") }
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
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(id: "hold-h2", match: HTTPRuleMatch(phase: .request, path: "/hold"),
                     action: .breakpoint),
            HTTPRule(id: "edit-h2", match: HTTPRuleMatch(phase: .request),
                     action: .editHeaders(remove: [], append: [HTTPHeaderField(name: "x-proof", value: "h2")]))
        ], limits: HTTPControlLimits()))
        let proxy = ProxyServer(certificateAuthority: root, sink: Release022Events(), group: group,
                                upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
                                egressPolicy: .init(allowInternal: true), controlRuntime: runtime, controlEventSink: controls)
        var channel: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let client = ProxyTestClient(group: group)
            let tunnel = try client.openTunnel(proxyPort: port, originHost: "localhost", originPort: origin.localPort)
            channel = tunnel
            let tls = try client.installTLS(on: tunnel, serverHostname: "localhost", mitmCACertificatePEM: root.caCertificatePEM,
                                            applicationProtocols: ["h2"], configuresHTTP2: true)
            try phase4BoundedWait(tls.installed)
            XCTAssertEqual(try phase4BoundedWait(tls.handshake), "h2")
            let multiplexer = try XCTUnwrap(try phase4BoundedWait(try XCTUnwrap(tls.multiplexer)))
            let held = try stage2H2Stream(multiplexer)
            try held.write(.headers(.init(headers: stage2H2Headers(port: origin.localPort, path: "/hold"), endStream: true)))
            let handle = try await controls.breakpoint()
            var siblings: [Release022H2Stream] = []
            for index in 0..<31 {
                let stream = try stage2H2Stream(multiplexer)
                try stream.write(.headers(.init(headers: stage2H2Headers(port: origin.localPort, path: "/sibling/\(index)"), endStream: true)))
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
            if case .alreadyCompleted = resolution {} else { XCTFail("Cancelled H2 stream accepted late breakpoint response") }
            let effectiveRequests = controls.events.filter { event in
                if case .effectiveHead(_, .request, let headers, _) = event {
                    headers.contains { $0.name == "x-proof" && $0.value == "h2" }
                } else { false }
            }
            XCTAssertEqual(effectiveRequests.count, 31)
            try await tunnel.close().get()
            try await proxy.stop()
            origin.stop()
            let usage = await runtime.resourceUsage()
            XCTAssertEqual(usage.pausedExchanges, 0)
            XCTAssertEqual(usage.retainedBytes, 0)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do { if let channel, channel.isActive { try await channel.close().get() }; try await proxy.stop() }
            catch { XCTFail("H2 control teardown failed: \(error)") }
            origin.stop()
            do { try await group.shutdownGracefully() } catch { XCTFail("H2 control group teardown failed: \(error)") }
            throw failure
        }
    }
}

import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Stage2ControlBoundaryAcceptanceTests: XCTestCase {
    func testTLSHTTP1EditAndExpiredBreakpointPreserveOriginIsolation() async throws {
        let peer = try Release022ExternalPeer()
        defer { peer.stop() }
        let ready = try await peer.start(script: "Stage2/lab_peer.py", arguments: ["server"])
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let authority = try CertificateAuthority.generate().authority
        let raw = Release022Events()
        let controls = Stage2ControlEvents()
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "tls-edit",
                match: HTTPRuleMatch(phase: .response, path: "/resource"),
                action: .editHeaders(remove: [], append: [HTTPHeaderField(name: "X-Boundary", value: "edited")])
            ),
            HTTPRule(id: "expire", match: HTTPRuleMatch(phase: .request, path: "/held"), action: .breakpoint)
        ], limits: HTTPControlLimits(), breakpointDeadlineSeconds: 1))
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: raw,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [try String(contentsOfFile: ready.root, encoding: .utf8)]),
            egressPolicy: .init(allowInternal: true),
            controlRuntime: runtime,
            controlEventSink: controls
        )
        do {
            let port = try await proxy.start(port: 0)
            let client = ProxyTestClient(group: group)
            let channel = try client.openTunnel(proxyPort: port, originHost: "localhost", originPort: ready.port)
            let tls = try client.installTLS(
                on: channel,
                serverHostname: "localhost",
                mitmCACertificatePEM: authority.caCertificatePEM,
                applicationProtocols: ["http/1.1"]
            )
            try phase4BoundedWait(tls.installed)
            XCTAssertEqual(try phase4BoundedWait(tls.handshake), "http/1.1")
            let received = Release022WireObservation()
            try phase4BoundedWait(channel.eventLoop.submit {
                try channel.pipeline.syncOperations.addHandler(Release022WireHandler(received))
            })
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(string:
                "GET /resource HTTP/1.1\r\nHost: localhost:\(ready.port)\r\n\r\n")))
            let payload = Array("SwiftMITM local replay resource\n".utf8)
            let firstResponse = try received.wait { $0.suffix(payload.count).elementsEqual(payload) }
            let firstText = try XCTUnwrap(String(bytes: firstResponse, encoding: .utf8))
            XCTAssertTrue(firstText.lowercased().contains("x-boundary: edited\r\n"))
            _ = try raw.wait { release022ResponseEnds($0).count == 1 }
            let clock = ContinuousClock()
            let started = clock.now
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(string:
                "GET /held HTTP/1.1\r\nHost: localhost:\(ready.port)\r\n\r\n")))
            let handle = try await controls.breakpoint()
            try received.waitForClose()
            XCTAssertGreaterThanOrEqual(stage2Milliseconds(started.duration(to: clock.now)), 1000)
            XCTAssertEqual(received.bytes, firstResponse)
            let late = await handle.resolve(.forwardOriginal)
            if case .alreadyCompleted = late {} else {
                XCTFail("Late resume revived an expired TLS request")
            }
            let originCount = try release022RunProcess(executable: "/usr/bin/curl", arguments: [
                "--silent", "--show-error", "--fail", "--max-time", "10", "--noproxy", "*",
                "--cacert", ready.root, "https://localhost:\(ready.port)/held-count"
            ])
            XCTAssertEqual(originCount, "0")
            XCTAssertEqual(release022ResponseEnds(raw.events).count, 1)
            try await proxy.stop()
            let usage = await runtime.resourceUsage()
            XCTAssertEqual(usage.pausedExchanges, 0)
            XCTAssertEqual(usage.taskSlots, 0)
            XCTAssertEqual(usage.retainedBytes, 0)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do { try await proxy.stop() } catch { XCTFail("Proxy teardown failed: \(error)") }
            do { try await group.shutdownGracefully() } catch { XCTFail("Group teardown failed: \(error)") }
            throw failure
        }
    }

    func testResponseResetAtByteOffsetPreservesHealthyFollowup() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "cut-response",
                match: HTTPRuleMatch(phase: .response, path: "/cut"),
                action: .fault(HTTPFault(direction: .response, resetAfterBytes: 4))
            )
        ], limits: HTTPControlLimits()))
        try await stage2WithInspectionWire(controlRuntime: runtime) { peer in
            let proxyPort = try XCTUnwrap(peer.client.remoteAddress?.port)
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/cut HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try peer.origin.write(Array("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789".utf8))
            try peer.received.waitForClose()
            let response = try XCTUnwrap(String(bytes: peer.received.bytes, encoding: .utf8))
            let parts = response.components(separatedBy: "\r\n\r\n")
            XCTAssertEqual(parts.count, 2)
            XCTAssertEqual(parts.last, "0123")
            let received = Release022WireObservation()
            let client = try release022WireClient(
                group: peer.group,
                port: proxyPort,
                observation: received
            )
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/healthy HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            let origin = try peer.origin.waitForConnection(at: 1)
            _ = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try phase4BoundedWait(origin.channel.writeAndFlush(ByteBuffer(string:
                "HTTP/1.1 204 No Content\r\n\r\n")))
            let healthy = try received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            XCTAssertTrue(try XCTUnwrap(String(bytes: healthy, encoding: .utf8)).contains("204 No Content"))
            XCTAssertEqual(peer.origin.acceptedConnections, 2)
            try await client.close().get()
        }
        let usage = await runtime.resourceUsage()
        XCTAssertEqual(usage.retainedBytes, 0)
    }
}

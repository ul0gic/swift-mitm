import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Stage2PassiveTLSCompatibilityTests: XCTestCase {
    func testPassiveTLSHTTP1RejectsExcessStreamWithoutEvictingHeldResponse() async throws {
        let peer = try Release022ExternalPeer()
        defer { peer.stop() }
        let ready = try await peer.start(script: "Stage2/lab_peer.py", arguments: ["server"])
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let root = try CertificateAuthority.generate().authority
        let capture = Release022Events()
        let admission =
            ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration(maximumStreams: 1))
        let proxy = ProxyServer(
            certificateAuthority: root,
            sink: capture,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [try String(contentsOfFile: ready.root, encoding: .utf8)]),
            egressPolicy: .init(allowInternal: true),
            admission: admission
        )
        do {
            let port = try await proxy.start(port: 0)
            let first = try connectedClient(group: group, proxyPort: port, originPort: ready.port, authority: root)
            try phase4BoundedWait(first.channel.writeAndFlush(ByteBuffer(string:
                "GET /held HTTP/1.1\r\nHost: localhost:\(ready.port)\r\n\r\n")))
            _ = try first.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            XCTAssertEqual(admission.usage.streams, 1)
            let second = try connectedClient(group: group, proxyPort: port, originPort: ready.port, authority: root)
            try phase4BoundedWait(second.channel.writeAndFlush(ByteBuffer(string:
                "GET /held HTTP/1.1\r\nHost: localhost:\(ready.port)\r\n\r\n")))
            try phase4BoundedWait(second.channel.closeFuture)
            XCTAssertTrue(first.channel.isActive)
            XCTAssertEqual(admission.usage.streams, 1)
            XCTAssertEqual(try originGet("/held-count", ready: ready), "1")
            XCTAssertEqual(try originGet("/release", ready: ready), "released")
            _ = try first.received.wait { $0.suffix(2).elementsEqual(Array("ok".utf8)) }
            _ = try capture.wait { release022ResponseEnds($0).count == 1 }
            try await stage2Await { admission.usage.streams == 0 }
            try await first.channel.close().get()
            try await proxy.stop()
            XCTAssertEqual(admission.usage.connections, 0)
            XCTAssertEqual(admission.usage.streams, 0)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do { try await proxy.stop() } catch { XCTFail("Proxy teardown failed: \(error)") }
            do { try await group.shutdownGracefully() } catch { XCTFail("Group teardown failed: \(error)") }
            throw failure
        }
    }

    private func connectedClient(
        group: EventLoopGroup,
        proxyPort: Int,
        originPort: Int,
        authority: CertificateAuthority
    ) throws -> (channel: Channel, received: Release022WireObservation) {
        let peer = ProxyTestClient(group: group)
        let channel = try peer.openTunnel(proxyPort: proxyPort, originHost: "localhost", originPort: originPort)
        let tls = try peer.installTLS(
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
        return (channel, received)
    }

    private func originGet(_ path: String, ready: Release022PeerReady) throws -> String {
        try release022RunProcess(executable: "/usr/bin/curl", arguments: [
            "--silent",
            "--show-error",
            "--fail",
            "--max-time",
            "10",
            "--noproxy",
            "*",
            "--cacert",
            ready.root,
            "https://localhost:\(ready.port)\(path)"
        ])
    }
}

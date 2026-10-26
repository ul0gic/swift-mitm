import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

func release022AssertIdentity(
    originName: String, serverName: String?, succeeds: Bool, ingress: ProxyIngress = .explicitConnect
) async throws {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
    let origin = try TLSOriginServer(
        group: group, bodySize: 4, applicationProtocols: ["http/1.1"], hostname: originName
    )
    let authority = try CertificateAuthority.generate().authority
    let sink = Release022Events()
    let proxy = ProxyServer(
        certificateAuthority: authority, sink: sink, group: group,
        upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
        egressPolicy: .init(allowInternal: true), ingress: ingress, captureBodyLimit: 4
    )
    var client: Channel?
    do {
        try origin.start()
        let port = try await proxy.start(port: 0)
        let peer = ProxyTestClient(group: group)
        let channel = try peer.openTunnel(proxyPort: port, originHost: "127.0.0.1", originPort: origin.localPort)
        client = channel
        let tls = try peer.installTLS(
            on: channel, serverHostname: serverName, mitmCACertificatePEM: authority.caCertificatePEM,
            applicationProtocols: ["http/1.1"]
        )
        try phase4BoundedWait(tls.installed)
        if succeeds {
            XCTAssertEqual(try phase4BoundedWait(tls.handshake), "http/1.1")
            let received = Release022WireObservation()
            try phase4BoundedWait(channel.eventLoop.submit {
                try channel.pipeline.syncOperations.addHandler(Release022WireHandler(received))
            })
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(string: "GET /identity HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try sink.wait { $0.contains { if case .responseEnd = $0 { true } else { false } } }
            let request = try XCTUnwrap(sink.events.compactMap { event -> CapturedRequestHead? in
                if case .requestHead(let head) = event { head } else { nil }
            }.first)
            XCTAssertEqual(request.target?.destination.address, "127.0.0.1")
            XCTAssertEqual(request.target?.destination.port, origin.localPort)
            XCTAssertEqual(request.target?.tlsServerName, serverName)
            XCTAssertFalse(sink.events.contains { if case .connectionFailure = $0 { true } else { false } })
        } else {
            do {
                _ = try phase4BoundedWait(tls.handshake)
                XCTFail("A trusted chain for the wrong selected identity must fail interception")
            } catch {
                _ = try sink.wait { $0.contains { if case .connectionFailure = $0 { true } else { false } } }
            }
            try phase4BoundedWait(channel.closeFuture)
        }
        if channel.isActive { try await channel.close().get() }
        try await proxy.stop()
        if !succeeds {
            XCTAssertEqual(sink.events.filter { if case .connectionFailure = $0 { true } else { false } }.count, 1)
            XCTAssertFalse(sink.events.contains { if case .opaqueOpen = $0 { true } else { false } })
            XCTAssertFalse(sink.events.contains { if case .requestHead = $0 { true } else { false } })
        }
        origin.stop()
        try await group.shutdownGracefully()
    } catch {
        if let client, client.isActive { try? await client.close().get() }
        try? await proxy.stop()
        origin.stop()
        try? await group.shutdownGracefully()
        throw error
    }
}

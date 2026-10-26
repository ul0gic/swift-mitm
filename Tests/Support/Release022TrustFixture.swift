import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

func release022AssertIdentity(
    originName: String,
    serverName: String?,
    succeeds: Bool,
    ingress: ProxyIngress = .explicitConnect
) async throws {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
    let origin = try TLSOriginServer(
        group: group,
        bodySize: 4,
        applicationProtocols: ["http/1.1"],
        hostname: originName
    )
    let authority = try CertificateAuthority.generate().authority
    let sink = Release022Events()
    let proxy = ProxyServer(
        certificateAuthority: authority,
        sink: sink,
        group: group,
        upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
        egressPolicy: .init(allowInternal: true),
        ingress: ingress,
        captureBodyLimit: 4
    )
    var client: Channel?
    do {
        try origin.start()
        let port = try await proxy.start(port: 0)
        let peer = ProxyTestClient(group: group)
        let channel = try peer.openTunnel(proxyPort: port, originHost: "127.0.0.1", originPort: origin.localPort)
        client = channel
        try release022ExerciseIdentity(
            channel: channel,
            peer: peer,
            authority: authority,
            sink: sink,
            expectation: .init(serverName: serverName, port: origin.localPort, succeeds: succeeds)
        )
        if channel.isActive {
            try await channel.close().get()
        }
        try await proxy.stop()
        if !succeeds {
            release022AssertIdentityFailure(sink.events)
        }
        origin.stop()
        try await group.shutdownGracefully()
    } catch {
        if let client, client.isActive {
            try? await client.close().get()
        }
        try? await proxy.stop()
        origin.stop()
        try? await group.shutdownGracefully()
        throw error
    }
}

private struct Release022IdentityExpectation {
    let serverName: String?
    let port: Int
    let succeeds: Bool
}

private func release022ExerciseIdentity(
    channel: Channel,
    peer: ProxyTestClient,
    authority: CertificateAuthority,
    sink: Release022Events,
    expectation: Release022IdentityExpectation
) throws {
    let tls = try peer.installTLS(
        on: channel,
        serverHostname: expectation.serverName,
        mitmCACertificatePEM: authority.caCertificatePEM,
        applicationProtocols: ["http/1.1"]
    )
    try phase4BoundedWait(tls.installed)
    if expectation.succeeds {
        XCTAssertEqual(try phase4BoundedWait(tls.handshake), "http/1.1")
        try release022AssertSelectedIdentity(
            channel: channel,
            port: expectation.port,
            serverName: expectation.serverName,
            sink: sink
        )
    } else {
        do {
            _ = try phase4BoundedWait(tls.handshake)
            XCTFail("A trusted chain for the wrong selected identity must fail interception")
        } catch {
            _ = try sink.wait { $0.contains {
                if case .connectionFailure = $0 {
                    true
                } else {
                    false
                }
            } }
        }
        try phase4BoundedWait(channel.closeFuture)
    }
}

private func release022AssertSelectedIdentity(
    channel: Channel,
    port: Int,
    serverName: String?,
    sink: Release022Events
) throws {
    let received = Release022WireObservation()
    try phase4BoundedWait(channel.eventLoop.submit {
        try channel.pipeline.syncOperations.addHandler(Release022WireHandler(received))
    })
    try phase4BoundedWait(channel
        .writeAndFlush(ByteBuffer(string: "GET /identity HTTP/1.1\r\nHost: localhost\r\n\r\n")))
    _ = try sink.wait { $0.contains {
        if case .responseEnd = $0 {
            true
        } else {
            false
        }
    } }
    let request = try XCTUnwrap(sink.events
        .compactMap { event -> CapturedRequestHead? in
            if case let .requestHead(head) = event {
                head
            } else {
                nil
            }
        }
        .first)
    XCTAssertEqual(request.target?.destination.address, "127.0.0.1")
    XCTAssertEqual(request.target?.destination.port, port)
    XCTAssertEqual(request.target?.tlsServerName, serverName)
    XCTAssertFalse(sink.events.contains {
        if case .connectionFailure = $0 {
            true
        } else {
            false
        }
    })
}

private func release022AssertIdentityFailure(_ events: [CaptureEvent]) {
    XCTAssertEqual(
        events
            .filter {
                if case .connectionFailure = $0 {
                    true
                } else {
                    false
                }
            }
            .count,
        1
    )
    XCTAssertFalse(events.contains {
        if case .opaqueOpen = $0 {
            true
        } else {
            false
        }
    })
    XCTAssertFalse(events.contains {
        if case .requestHead = $0 {
            true
        } else {
            false
        }
    })
}

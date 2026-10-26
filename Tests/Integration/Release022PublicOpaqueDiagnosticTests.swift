import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicOpaqueDiagnosticTests: XCTestCase {
    func testSelectedOpaqueConnectFailureHasOneConnectStageDiagnostic() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Release022WireOrigin(group: group)
        let sink = Release022Events()
        var proxy: ProxyServer?
        var client: Channel?
        do {
            try origin.start()
            let originPort = origin.port
            origin.stop()
            let target = "127.0.0.1:\(originPort)"
            let policy = try XCTUnwrap(ProxyInterceptionPolicy(passThroughAuthorities: [target]))
            let server = ProxyServer(
                certificateAuthority: try CertificateAuthority.generate().authority,
                sink: sink, group: group, egressPolicy: .init(allowInternal: true),
                interceptionPolicy: policy
            )
            proxy = server
            let port = try await server.start(port: 0)
            let received = Release022WireObservation()
            let channel = try release022WireClient(group: group, port: port, observation: received)
            client = channel
            let request = "CONNECT \(target) HTTP/1.1\r\nHost: \(target)\r\n\r\n"
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(string: request)))
            try received.waitForClose()
            try await server.stop()
            let failures = sink.events.compactMap { event -> CapturedConnectionFailure? in
                if case .connectionFailure(let failure) = event {
                    failure
                } else {
                    nil
                }
            }
            XCTAssertEqual(failures.count, 1)
            XCTAssertEqual(failures.first?.stage, .connect)
            XCTAssertEqual(failures.first?.code, .upstreamConnectFailed)
            XCTAssertEqual(failures.first?.target?.destination.port, originPort)
            XCTAssertFalse(sink.events.contains {
                if case .opaqueOpen = $0 {
                    true
                } else {
                    false
                }
            })
            if channel.isActive {
                try await channel.close().get()
            }
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            if let proxy {
                try? await proxy.stop()
            }
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }
}

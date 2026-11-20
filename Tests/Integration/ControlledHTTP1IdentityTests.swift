import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class ControlledHTTP1IdentityTests: XCTestCase {
    func testIPTunnelPreservesSNIAndAuthenticatesRemappedOrigin() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let name = "virtual.example"
        let origin = try TLSOriginServer(group: group, bodySize: 7, applicationProtocols: ["http/1.1"], hostname: name)
        let remapped = try TLSOriginServer(group: group, bodySize: 11, applicationProtocols: ["http/1.1"])
        let originalNames = NIOLockedValueBox<[String?]>([])
        let mappedNames = NIOLockedValueBox<[String?]>([])
        let authority = try CertificateAuthority.generate().authority
        let runtime = HTTPControlRuntime(
            configuration: try HTTPControlConfiguration(rules: [], limits: HTTPControlLimits())
        )
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: Release022Events(),
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM, remapped.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            controlRuntime: runtime
        )
        do {
            try origin.start { sni in originalNames.withLockedValue { $0.append(sni) } }
            try remapped.start { sni in mappedNames.withLockedValue { $0.append(sni) } }
            let routes = [("/mapped", remapped.localPort), ("/wrong-name", origin.localPort)]
            let rules = routes.map { path, port in
                HTTPRule(
                    id: path,
                    match: HTTPRuleMatch(phase: .request, path: path),
                    action: .route(authority: "localhost:\(port)", sensitiveHeaders: [])
                )
            }
            try await runtime.replaceConfiguration(HTTPControlConfiguration(rules: rules, limits: HTTPControlLimits()))
            let proxyPort = try await proxy.start(port: 0)
            let client = ProxyTestClient(group: group)
            let tunnel = try client.openTunnel(
                proxyPort: proxyPort,
                originHost: "127.0.0.1",
                originPort: origin.localPort
            )
            defer { tunnel.close(promise: nil) }
            let tls = try client.installTLS(
                on: tunnel,
                serverHostname: name,
                mitmCACertificatePEM: authority.caCertificatePEM,
                applicationProtocols: ["http/1.1"]
            )
            try phase4BoundedWait(tls.installed)
            XCTAssertEqual(try phase4BoundedWait(tls.handshake), "http/1.1")
            let wire = Release022WireObservation()
            try phase4BoundedWait(tunnel.eventLoop.submit {
                try tunnel.pipeline.syncOperations.addHandler(Release022WireHandler(wire))
            })
            for (path, status, size) in [("/", 200, 7), ("/mapped", 200, 11), ("/wrong-name", 502, 0)] {
                wire.reset()
                let request = "GET \(path) HTTP/1.1\r\nHost: 127.0.0.1:\(origin.localPort)\r\n\r\n"
                try phase4BoundedWait(tunnel.writeAndFlush(ByteBuffer(string: request)))
                let end = Array(("\r\n\r\n" + String(repeating: "A", count: size)).utf8)
                let response = try wire.wait { $0.suffix(end.count).elementsEqual(end) }
                XCTAssertTrue(response.starts(with: "HTTP/1.1 \(status) ".utf8))
            }
            XCTAssertEqual(originalNames.withLockedValue { $0 }, [name, name, "localhost"])
            XCTAssertEqual(mappedNames.withLockedValue { $0 }, ["localhost"])
            try await proxy.stop()
        } catch {
            try? await proxy.stop()
            origin.stop()
            remapped.stop()
            try? await group.shutdownGracefully()
            throw error
        }
        origin.stop()
        remapped.stop()
        try await group.shutdownGracefully()
    }
}

import Foundation
import NIOPosix
import XCTest

import SwiftMITM

final class Release022IndependentGRPCCompatibilityTests: XCTestCase {
    func testIndependentGRPCClientAndServerThroughPublicProxy() async throws {
        let peer = try Release022ExternalPeer()
        defer { peer.stop() }
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Release022Events()
        var proxy: ProxyServer?
        do {
            let ready = try await peer.start(script: "grpc_peer.py", arguments: ["server"])
            let originRoot = try String(contentsOfFile: ready.root, encoding: .utf8)
            let authority = try CertificateAuthority.generate().authority
            let proxyRoot = peer.directory.appendingPathComponent("proxy-ca.pem")
            try Data(authority.caCertificatePEM.utf8).write(to: proxyRoot)
            let server = ProxyServer(
                certificateAuthority: authority,
                sink: sink,
                group: group,
                upstreamPolicy: .init(additionalTrustRootsPEM: [originRoot]),
                egressPolicy: .init(allowInternal: true),
                captureBodyLimit: 65_536
            )
            proxy = server
            let port = try await server.start(port: 0)
            let result = try independentClient(peer: peer, port: port, originPort: ready.port, proxyRoot: proxyRoot)
            FileHandle.standardError.write(Data("CLIENT-MATRIX \(result)".utf8))
            XCTAssertTrue(result.contains("trailers-only"))
            let events = try sink.wait {
                $0.filter {
                    if case .responseEnd = $0 {
                        true
                    } else {
                        false
                    }
                }
                .count == 5
            }
            assertIndependentGRPCEvents(events)
            try await server.stop()
            try await group.shutdownGracefully()
        } catch {
            if let proxy {
                try? await proxy.stop()
            }
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func independentClient(
        peer: Release022ExternalPeer,
        port: Int,
        originPort: Int,
        proxyRoot: URL
    ) throws -> String {
        var environment = ProcessInfo.processInfo.environment
        environment["grpc_proxy"] = "http://127.0.0.1:\(port)"
        environment["https_proxy"] = "http://127.0.0.1:\(port)"
        environment["http_proxy"] = "http://127.0.0.1:\(port)"
        environment["no_grpc_proxy"] = ""
        environment["no_proxy"] = ""
        environment["NO_PROXY"] = ""
        return try release022RunProcess(executable: peer.python, arguments: [
            Release022ExternalPeer.clientsDirectory.appendingPathComponent("grpc_peer.py").path,
            "client",
            "--target",
            "localhost:\(originPort)",
            "--root",
            proxyRoot.path
        ], environment: environment)
    }

    private func assertIndependentGRPCEvents(_ events: [CaptureEvent]) {
        let requests = events.compactMap { event -> CapturedRequestHead? in
            if case let .requestHead(head) = event {
                head
            } else {
                nil
            }
        }
        XCTAssertEqual(Set(requests.map(\.path)), Set([
            "/independent.Transport/Unary",
            "/independent.Transport/ServerStream",
            "/independent.Transport/ClientStream",
            "/independent.Transport/Bidi",
            "/independent.Transport/Denied"
        ]))
        XCTAssertTrue(requests.allSatisfy { $0.version == .http2 })
        XCTAssertFalse(events.contains {
            if case .streamError = $0 {
                true
            } else {
                false
            }
        })
    }
}

import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Release022ClientCompatibilityTests: XCTestCase {
    func testCurlHTTPAndHTTPSWithExplicitTrust() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let clear = try TLSOriginServer(group: group, bodySize: 32, cleartext: true)
        let secure = try TLSOriginServer(group: group, bodySize: 32, applicationProtocols: ["http/1.1"])
        let authority = try CertificateAuthority.generate().authority
        let sink = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [secure.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            ingress: .explicitHTTP
        )
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let certificate = directory.appendingPathComponent("proxy-ca.pem")
        try Data(authority.caCertificatePEM.utf8).write(to: certificate)
        do {
            try clear.start()
            try secure.start()
            let port = try await proxy.start(port: 0)
            try assertCurlRequests(
                port: port,
                urls: ["http://localhost:\(clear.localPort)/", "https://localhost:\(secure.localPort)/"],
                certificate: certificate
            )
            try assertCurlCapture(sink)
            try await proxy.stop()
            clear.stop()
            secure.stop()
            try await group.shutdownGracefully()
        } catch {
            try? await proxy.stop()
            clear.stop()
            secure.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testURLSessionHTTP11WithExplicitTrustAndProxy() async throws {
        try await urlSession(protocolName: "http/1.1", version: .http11)
    }

    func testURLSessionHTTP2WithExplicitTrustAndProxy() async throws {
        try await urlSession(protocolName: "h2", version: .http2)
    }

    func testURLSessionHTTP2WithQualifiedHostnameAndExplicitTrustAndProxy() async throws {
        let host = try XCTUnwrap(
            ProcessInfo.processInfo.environment["SWIFTMITM_TEST_DNS_HOST"],
            "Compatibility CI must provide SWIFTMITM_TEST_DNS_HOST and its owned hosts-file mapping"
        )
        guard host.hasSuffix(".test"), !host.dropLast(5).isEmpty else {
            XCTFail("SWIFTMITM_TEST_DNS_HOST must be a qualified fixture hostname under .test")
            return
        }
        try await urlSession(protocolName: "h2", version: .http2, logicalHost: host)
    }

    func testURLSessionHTTP2WithSessionProxyDictionaryDiagnostic() async throws {
        try await urlSession(protocolName: "h2", version: .http2, routing: .sessionDictionary)
    }

    func testURLSessionHTTP2WithAlternateLoaderDiagnostic() async throws {
        try await urlSession(protocolName: "h2", version: .http2, routing: .nativeAlternateLoader)
    }

    private func urlSession(
        protocolName: String,
        version: HTTPProtocolVersion,
        routing: Release022ProxyRouting = .native,
        logicalHost: String? = nil
    ) async throws {
        let bindAddress = try release022RunnerIPv4()
        let originHost = logicalHost ?? bindAddress
        guard try assertURLSessionHost(logicalHost, bindAddress: bindAddress) else {
            return
        }
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(
            group: group,
            bodySize: 32,
            applicationProtocols: [protocolName],
            hostname: originHost
        )
        let authority = try CertificateAuthority.generate().authority
        let sink = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            ingress: .explicitHTTP
        )
        var session: URLSession?
        do {
            try origin.start(bindAddress: bindAddress)
            let port = try await proxy.start(port: 0)
            let client = try release022TrustedURLSession(
                port: port,
                authority: authority,
                originHost: originHost,
                originPort: origin.localPort,
                protocolName: protocolName,
                routing: routing
            )
            session = client
            try await release022AssertURLSessionBody(client, host: originHost, port: origin.localPort)
            try assertURLSessionCapture(sink, protocolName: protocolName, version: version)
            try release022AssertURLSessionRouting(client)
            client.invalidateAndCancel()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            reportURLSessionPath(sink)
            session?.invalidateAndCancel()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertURLSessionHost(_ logicalHost: String?, bindAddress: String) throws -> Bool {
        guard let logicalHost else { return true }
        let resolved = try SocketAddress.makeAddressResolvingHost(logicalHost, port: 0)
        guard resolved.ipAddress == bindAddress else {
            XCTFail(
                "Fixture hostname resolved to \(String(describing: resolved.ipAddress)); expected \(bindAddress)"
            )
            return false
        }
        return true
    }

    private func reportURLSessionPath(_ sink: Release022Events) {
        let failures = sink.events.compactMap { event -> String? in
            if case let .connectionFailure(failure) = event {
                return "\(failure.reason):\(String(describing: failure.code))"
            }
            return nil
        }
        XCTFail(
            "URLSession path diagnostics: captureEvents=\(sink.events.count), failures=\(Array(failures.prefix(4)))"
        )
    }

    private func assertCurlCapture(_ sink: Release022Events) throws {
        let events = try sink.wait { $0.filter {
            if case .responseEnd = $0 {
                true
            } else {
                false
            }
        }
        .count == 2
        }
        let schemes = events.compactMap { event -> String? in
            if case let .requestHead(head) = event {
                head.scheme
            } else {
                nil
            }
        }
        XCTAssertEqual(schemes, ["http", "https"])
    }

    private func assertURLSessionCapture(
        _ sink: Release022Events,
        protocolName: String,
        version: HTTPProtocolVersion
    ) throws {
        let events = try sink.wait { $0.contains(where: release022IsResponseEnd) }
        let heads = events.compactMap { event -> CapturedRequestHead? in
            if case let .requestHead(head) = event {
                head
            } else {
                nil
            }
        }
        XCTAssertEqual(heads.count, 1)
        XCTAssertEqual(heads.first?.version, version)
        FileHandle.standardError
            .write(
                Data(
                    ("CLIENT-MATRIX URLSession protocol=\(protocolName) "
                        + "os=\(ProcessInfo.processInfo.operatingSystemVersionString) trusted=true\n").utf8
                )
            )
    }

    private func assertCurlRequests(port: Int, urls: [String], certificate: URL) throws {
        let version = try curl(["--version"])
        FileHandle.standardError
            .write(
                Data(
                    "CLIENT-MATRIX curl=\(version.output) os=\(ProcessInfo.processInfo.operatingSystemVersionString)\n"
                        .utf8
                )
            )
        for url in urls {
            let result = try curl([
                "--silent",
                "--show-error",
                "--fail",
                "--max-time",
                "10",
                "--noproxy",
                "",
                "--proxy",
                "http://127.0.0.1:\(port)",
                "--cacert",
                certificate.path,
                url
            ])
            XCTAssertEqual(result.status, 0, result.error)
            XCTAssertEqual(result.output, String(repeating: "A", count: 32))
        }
    }

    private struct CurlResult {
        let status: Int32
        let output: String
        let error: String
    }

    private func curl(_ arguments: [String]) throws -> CurlResult {
        let process = Process()
        let output = Pipe()
        let error = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/curl")
        process.arguments = arguments
        process.standardOutput = output
        process.standardError = error
        try process.run()
        process.waitUntilExit()
        return CurlResult(
            status: process.terminationStatus,
            output: try XCTUnwrap(String(data: output.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)),
            error: try XCTUnwrap(String(data: error.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8))
        )
    }
}

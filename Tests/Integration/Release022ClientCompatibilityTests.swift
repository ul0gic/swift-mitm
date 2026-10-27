import Foundation
import Network
import NIOPosix
import NIOSSL
import Security
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

    private func urlSession(protocolName: String, version: HTTPProtocolVersion) async throws {
        let originHost = try release022RunnerIPv4()
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
            try origin.start(bindAddress: originHost)
            let port = try await proxy.start(port: 0)
            let client = try trustedURLSession(port: port, authority: authority, originHost: originHost)
            session = client
            let url = try XCTUnwrap(URL(string: "https://\(originHost):\(origin.localPort)/"))
            let (data, response) = try await client.data(from: url)
            XCTAssertEqual(data, Data(repeating: 0x41, count: 32))
            XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)
            try assertURLSessionCapture(sink, protocolName: protocolName, version: version)
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

    private func trustedURLSession(port: Int, authority: CertificateAuthority, originHost: String) throws -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 10
        let proxyPort = try XCTUnwrap(UInt16(exactly: port))
        let endpointPort = try XCTUnwrap(NWEndpoint.Port(rawValue: proxyPort))
        var proxyConfiguration = ProxyConfiguration(httpCONNECTProxy: .hostPort(host: "127.0.0.1", port: endpointPort))
        proxyConfiguration.matchDomains = [originHost]
        configuration.proxyConfigurations = [proxyConfiguration]
        let der = try NIOSSLCertificate(bytes: Array(authority.caCertificatePEM.utf8), format: .pem).toDERBytes()
        let root = try XCTUnwrap(SecCertificateCreateWithData(nil, Data(der) as CFData))
        let delegate = Release022URLSessionTrust(root: root)
        return URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
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

private final class Release022URLSessionTrust: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    let root: SecCertificate

    init(root: SecCertificate) { self.root = root }

    func urlSession(_ session: URLSession, task: URLSessionTask, didFinishCollecting metrics: URLSessionTaskMetrics) {
        for (index, transaction) in metrics.transactionMetrics.prefix(4).enumerated() {
            let facts = "transaction=\(index), proxy=\(transaction.isProxyConnection), "
                + "protocol=\(String(describing: transaction.networkProtocolName))"
            FileHandle.standardError.write(Data("URLSession route metrics: \(facts)\n".utf8))
        }
    }

    func urlSession(
        _ session: URLSession,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust else {
            XCTFail("URLSession unexpected authentication method: \(challenge.protectionSpace.authenticationMethod)")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        guard let trust = challenge.protectionSpace.serverTrust else {
            XCTFail("URLSession server-trust challenge omitted trust")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        let anchorStatus = SecTrustSetAnchorCertificates(trust, [root] as CFArray)
        let exclusiveStatus = SecTrustSetAnchorCertificatesOnly(trust, true)
        guard anchorStatus == errSecSuccess, exclusiveStatus == errSecSuccess else {
            XCTFail("URLSession custom-root setup failed: anchor=\(anchorStatus), exclusive=\(exclusiveStatus)")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        var failure: CFError?
        guard SecTrustEvaluateWithError(trust, &failure) else {
            let facts = release022TrustDiagnostic(trust: trust, expectedRoot: root, failure: failure)
            XCTFail("URLSession trust denied: \(facts); host=\(challenge.protectionSpace.host)")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        completionHandler(.useCredential, URLCredential(trust: trust))
    }
}

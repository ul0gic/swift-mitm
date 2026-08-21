import Foundation
import NIOPosix
import XCTest

@testable import SwiftMITM

final class AppleTLSProxyCompatibilityTests: XCTestCase {
    private struct NoopSink: CaptureEventSink {
        func receive(_ event: CaptureEvent) {}
    }

    func testSecureTransportClientCompletesTLS12ThroughPublicProxy() async throws {
        let curlVersion = try runCurl(arguments: ["-V"])
        guard curlVersion.standardOutput.contains("(SecureTransport)") else {
            throw XCTSkip("system curl does not use SecureTransport")
        }

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(
            group: group,
            bodySize: 32,
            applicationProtocols: ["http/1.1"]
        )
        let authority = try CertificateAuthority.generate().authority
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: NoopSink(),
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let result = try fetchThroughProxy(proxyPort: proxyPort, originPort: origin.localPort)

            XCTAssertEqual(result.terminationStatus, 0, result.standardError)
            XCTAssertEqual(result.standardOutput, "200")
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func fetchThroughProxy(proxyPort: Int, originPort: Int) throws -> ProcessResult {
        try runCurl(arguments: [
            "--silent",
            "--show-error",
            "--noproxy",
            "",
            "--proxy",
            "http://127.0.0.1:\(proxyPort)",
            "--insecure",
            "--tlsv1.2",
            "--tls-max",
            "1.2",
            "--max-time",
            "10",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            "https://localhost:\(originPort)/"
        ])
    }

    private func runCurl(arguments: [String]) throws -> ProcessResult {
        let process = Process()
        let standardOutput = Pipe()
        let standardError = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/curl")
        process.arguments = arguments
        process.standardOutput = standardOutput
        process.standardError = standardError
        try process.run()
        process.waitUntilExit()
        let outputData = standardOutput.fileHandleForReading.readDataToEndOfFile()
        let errorData = standardError.fileHandleForReading.readDataToEndOfFile()
        return ProcessResult(
            terminationStatus: process.terminationStatus,
            standardOutput: String(bytes: outputData, encoding: .utf8) ?? "",
            standardError: String(bytes: errorData, encoding: .utf8) ?? ""
        )
    }
}

private struct ProcessResult {
    let terminationStatus: Int32
    let standardOutput: String
    let standardError: String
}

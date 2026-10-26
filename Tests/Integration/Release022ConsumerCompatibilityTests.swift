import Darwin
import Foundation
import NIOPosix
import XCTest

import SwiftMITM

final class Release022ConsumerCompatibilityTests: XCTestCase {
    func testExternalFirstCaptureCapturesHTTPAndHTTPSAndDrainsOnStop() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let clear = try TLSOriginServer(group: group, bodySize: 8, cleartext: true)
        let secure = try TLSOriginServer(group: group, bodySize: 8, applicationProtocols: ["http/1.1"])
        let example = try Release022ExampleProcess()
        defer { example.stop() }
        do {
            try clear.start()
            try secure.start()
            let authority = try CertificateAuthority.generate()
            let port = try example.start(
                root: authority,
                originRoot: secure.caCertificatePEM,
                asynchronous: true,
                captureBytes: 8
            )
            let certificate = example.directory.appendingPathComponent("proxy-ca.pem")
            for url in ["http://localhost:\(clear.localPort)/", "https://localhost:\(secure.localPort)/"] {
                let body = try release022RunProcess(executable: "/usr/bin/curl", arguments: [
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
                XCTAssertEqual(body, String(repeating: "A", count: 8))
            }
            example.requestStop(signal: SIGINT)
            let records = try example.wait { $0.contains { $0["type"] as? String == "stopped" } }
            XCTAssertEqual(try example.waitForExit(timeout: 2), 0)
            XCTAssertEqual(records.filter { $0["type"] as? String == "requestHead" }.count, 2)
            XCTAssertEqual(records.filter { $0["type"] as? String == "responseEnd" }.count, 2)
            XCTAssertFalse(records.contains { $0["type"] as? String == "deliveryLoss" })
            clear.stop()
            secure.stop()
            try await group.shutdownGracefully()
        } catch {
            clear.stop()
            secure.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testAsyncConsumerByteOverflowReportsTerminalLossWhileForwardingCompletes() async throws {
        try await overflow(bodySize: 32 * 1024 * 1024, captureBytes: 2 * 1024 * 1024, requests: 1)
    }

    func testAsyncConsumerEventOverflowReportsTerminalLossWhileForwardingCompletes() async throws {
        try await overflow(bodySize: 0, captureBytes: 0, requests: 512)
    }

    func testSIGTERMExitsWithUnreadStdoutAfterProxyForwardingCompletes() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 1024 * 1024, cleartext: true)
        let example = try Release022ExampleProcess()
        defer { example.stop() }
        do {
            try origin.start()
            let port = try example.start(
                root: try CertificateAuthority.generate(),
                originRoot: origin.caCertificatePEM,
                asynchronous: true,
                captureBytes: 512 * 1024
            )
            example.pauseOutput()
            let delivered = try release022RunProcess(executable: "/usr/bin/curl", arguments: [
                "--silent",
                "--show-error",
                "--fail",
                "--max-time",
                "10",
                "--noproxy",
                "",
                "--proxy",
                "http://127.0.0.1:\(port)",
                "--output",
                "/dev/null",
                "--write-out",
                "%{size_download}",
                "http://localhost:\(origin.localPort)/held-output"
            ])
            XCTAssertEqual(delivered, "1048576")
            example.requestStop(signal: SIGTERM)
            XCTAssertEqual(try example.waitForExit(timeout: 2), 0)
            let records = try example.wait { records in
                records.contains {
                    $0["_stream"] as? String == "stderr" && $0["type"] as? String == "deliveryLoss"
                }
            }
            let losses = records.filter {
                $0["_stream"] as? String == "stderr" && $0["type"] as? String == "deliveryLoss"
            }
            XCTAssertEqual(losses.count, 1)
            XCTAssertEqual(losses.first?["reason"] as? String, "cancelled")
            XCTAssertEqual(losses.first?["allActiveCapturesIncomplete"] as? Bool, true)
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testBrokenStdoutReportsOutputFailureAndStillForwardsUntilSIGTERM() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 8, cleartext: true)
        let example = try Release022ExampleProcess()
        defer { example.stop() }
        do {
            try origin.start()
            let port = try example.start(
                root: try CertificateAuthority.generate(),
                originRoot: origin.caCertificatePEM,
                asynchronous: true,
                captureBytes: 8
            )
            try example.closeOutputReader()
            let body = try release022RunProcess(executable: "/usr/bin/curl", arguments: [
                "--silent",
                "--show-error",
                "--fail",
                "--max-time",
                "10",
                "--noproxy",
                "",
                "--proxy",
                "http://127.0.0.1:\(port)",
                "http://localhost:\(origin.localPort)/broken-output"
            ])
            XCTAssertEqual(body, "AAAAAAAA")
            _ = try example.wait { records in
                records.contains {
                    $0["_stream"] as? String == "stderr" && $0["reason"] as? String == "outputFailed"
                }
            }
            example.requestStop(signal: SIGTERM)
            XCTAssertEqual(try example.waitForExit(timeout: 2), 0)
            let records = try example.wait { records in
                records.contains {
                    $0["_stream"] as? String == "stderr" && $0["type"] as? String == "deliveryLoss"
                }
            }
            let losses = records.filter {
                $0["_stream"] as? String == "stderr" && $0["type"] as? String == "deliveryLoss"
            }
            XCTAssertEqual(losses.count, 1)
            XCTAssertEqual(losses.first?["reason"] as? String, "outputFailed")
            XCTAssertEqual(losses.first?["allActiveCapturesIncomplete"] as? Bool, true)
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func overflow(bodySize: Int, captureBytes: Int, requests: Int) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: bodySize, cleartext: true)
        let example = try Release022ExampleProcess()
        defer { example.stop() }
        do {
            try origin.start()
            let authority = try CertificateAuthority.generate()
            let port = try example.start(
                root: authority,
                originRoot: origin.caCertificatePEM,
                asynchronous: true,
                captureBytes: captureBytes
            )
            example.pauseOutput()
            for index in 0 ..< requests {
                _ = try release022RunProcess(executable: "/usr/bin/curl", arguments: [
                    "--silent",
                    "--show-error",
                    "--fail",
                    "--max-time",
                    "10",
                    "--noproxy",
                    "",
                    "--proxy",
                    "http://127.0.0.1:\(port)",
                    "--output",
                    "/dev/null",
                    "http://localhost:\(origin.localPort)/\(index)"
                ])
            }
            example.resumeOutput()
            let records = try example.wait { records in
                records.contains {
                    $0["_stream"] as? String == "stderr" && $0["type"] as? String == "deliveryLoss"
                }
            }
            let losses = records.filter {
                $0["_stream"] as? String == "stderr" && $0["type"] as? String == "deliveryLoss"
            }
            XCTAssertEqual(losses.count, 1)
            XCTAssertEqual(losses.first?["reason"] as? String, "overflow")
            XCTAssertEqual(losses.first?["allActiveCapturesIncomplete"] as? Bool, true)
            example.requestStop()
            XCTAssertEqual(try example.waitForExit(timeout: 2), 0)
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }
}

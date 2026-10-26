import Foundation
import NIOPosix
import XCTest

import SwiftMITM

final class Stage2HTTP2FaultCompatibilityTests: XCTestCase {
    private struct Report: Decodable {
        let responseBytes: Int
        let headDelayMilliseconds: Double
        let elapsedMilliseconds: Double
        let siblingBeforeCompletion: Bool
        let cancelled: Bool
        let siblingAfterCancel: Bool
    }

    func testIndependentHTTP2ThrottlePreservesLargeTerminalResponseSiblingsAndCancellation() async throws {
        let peer = try Release022ExternalPeer()
        defer { peer.stop() }
        let ready = try await peer.start(script: "Stage2/h2_fault_peer.py", arguments: ["server"])
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let authority = try CertificateAuthority.generate().authority
        let proxyRoot = peer.directory.appendingPathComponent("proxy-ca.pem")
        try Data(authority.caCertificatePEM.utf8).write(to: proxyRoot)
        let runtime = try faultRuntime()
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        let capture = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: capture,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [try String(contentsOfFile: ready.root, encoding: .utf8)]),
            egressPolicy: .init(allowInternal: true),
            controlRuntime: runtime,
            admission: admission
        )
        do {
            let port = try await proxy.start(port: 0)
            let output = try runFaultClient(peer: peer, port: port, originPort: ready.port, proxyRoot: proxyRoot)
            try assertFaultReport(output, capture: capture)
            try await stage2AwaitAsync {
                let usage = await runtime.resourceUsage()
                return usage.retainedBytes == 0 && admission.usage.retainedBytes == 0 && admission.usage.streams == 0
            }
            try await proxy.stop()
            XCTAssertEqual(admission.usage.connections, 0)
            try await group.shutdownGracefully()
        } catch {
            let failure = error
            do { try await proxy.stop() } catch { XCTFail("Proxy teardown failed: \(error)") }
            do { try await group.shutdownGracefully() } catch { XCTFail("Group teardown failed: \(error)") }
            throw failure
        }
    }


    private func runFaultClient(peer: Release022ExternalPeer, port: Int, originPort: Int, proxyRoot: URL) throws -> String {
        var environment = ProcessInfo.processInfo.environment
        environment["grpc_proxy"] = "http://127.0.0.1:\(port)"
        environment["https_proxy"] = "http://127.0.0.1:\(port)"
        environment["no_grpc_proxy"] = ""
        environment["no_proxy"] = ""
        environment["NO_PROXY"] = ""
        return try release022RunProcess(executable: peer.python, arguments: [
            Release022ExternalPeer.clientsDirectory.appendingPathComponent("Stage2/h2_fault_peer.py").path,
            "client",
            "--target",
            "localhost:\(originPort)",
            "--root",
            proxyRoot.path
        ], environment: environment)
    }

    private func faultRuntime() throws -> HTTPControlRuntime {
        let fault = try HTTPFault(
            direction: .response,
            delayMilliseconds: 1500,
            bytesPerSecond: 65_536,
            burstBytes: 1024
        )
        return HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "slow",
                match: HTTPRuleMatch(phase: .response, path: "/stage2.Fault/Slow"),
                action: .fault(fault)
            ),
            HTTPRule(
                id: "cancel",
                match: HTTPRuleMatch(phase: .response, path: "/stage2.Fault/Cancel"),
                action: .fault(fault)
            )
        ], limits: HTTPControlLimits()))
    }

    private func assertFaultReport(_ output: String, capture: Release022Events) throws {
        let report = try JSONDecoder().decode(Report.self, from: Data(output.utf8))
        XCTAssertEqual(report.responseBytes, 262_146)
        XCTAssertGreaterThanOrEqual(report.headDelayMilliseconds, 1500)
        XCTAssertGreaterThanOrEqual(report.elapsedMilliseconds, 1500 + Double(262_146 - 1024) / 65_536 * 1000)
        XCTAssertTrue(report.siblingBeforeCompletion)
        XCTAssertTrue(report.cancelled)
        XCTAssertTrue(report.siblingAfterCancel)
        let slow = try XCTUnwrap(release022Requests(capture.events).first { $0.path == "/stage2.Fault/Slow" })
        XCTAssertEqual(slow.version, .http2)
        XCTAssertFalse(capture.events.contains {
            if case let .streamError(id, _) = $0 {
                id == slow.id
            } else {
                false
            }
        })
    }
}

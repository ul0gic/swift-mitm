import AppKit
import Foundation
import Network
import NIOPosix
import NIOSSL
import Security
import WebKit
import XCTest

import SwiftMITM

@MainActor
final class Release022BrowserCompatibilityTests: XCTestCase {
    func testWKWebViewSSEAndWebSocketThroughPublicProxyWithExplicitTrust() async throws {
        let peer = try Release022ExternalPeer()
        defer { peer.stop() }
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Release022Events()
        var proxy: ProxyServer?
        var webView: WKWebView?
        do {
            let ready = try await peer.start(script: "browser_peer.py")
            let authority = try CertificateAuthority.generate().authority
            let server = try makeProxy(group: group, ready: ready, authority: authority, sink: sink)
            proxy = server
            let port = try await server.start(port: 0)
            let (configuration, root) = try browserConfiguration(port: port, authority: authority)
            let completion = Phase2FixtureCompletion<String>(eventLoop: group.next(), timeout: .seconds(20))
            let observer = Release022BrowserObserver(root: root, completion: completion)
            configuration.userContentController.add(observer, name: "result")
            _ = NSApplication.shared
            let view = WKWebView(frame: NSRect(x: 0, y: 0, width: 800, height: 600), configuration: configuration)
            webView = view
            view.navigationDelegate = observer
            let url = try XCTUnwrap(URL(string: "https://localhost:\(ready.port)/"))
            view.load(URLRequest(url: url))
            let result = try await completion.futureResult.get()
            XCTAssertEqual(result, "passed")
            try assertBrowserCapture(sink)
            FileHandle.standardError
                .write(
                    Data(
                        "CLIENT-MATRIX WKWebView os=\(ProcessInfo.processInfo.operatingSystemVersionString) trusted=true sse=true websocket=true\n"
                .utf8
                    )
                )
            view.stopLoading()
            configuration.userContentController.removeScriptMessageHandler(forName: "result")
            view.navigationDelegate = nil
            try await server.stop()
            try await group.shutdownGracefully()
        } catch {
            webView?.stopLoading()
            webView?.configuration.userContentController.removeScriptMessageHandler(forName: "result")
            webView?.navigationDelegate = nil
            if let proxy {
                try? await proxy.stop()
            }
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func testChromiumSSEAndWebSocketThroughPublicProxyWithRunnerTrust() async throws {
        guard let executable = ProcessInfo.processInfo.environment["SWIFTMITM_CHROMIUM_PATH"] else {
            throw Release022ClientError.missingEnvironment("SWIFTMITM_CHROMIUM_PATH")
        }
        let peer = try Release022ExternalPeer()
        defer { peer.stop() }
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Release022Events()
        var proxy: ProxyServer?
        var trust: Release022RunnerTrust?
        do {
            let ready = try await peer.start(script: "browser_peer.py")
            let authority = try CertificateAuthority.generate().authority
            let certificate = peer.directory.appendingPathComponent("proxy-ca.pem")
            try Data(authority.caCertificatePEM.utf8).write(to: certificate)
            let runnerTrust = Release022RunnerTrust(certificate: certificate, directory: peer.directory)
            trust = runnerTrust
            try runnerTrust.install()
            let server = try makeProxy(group: group, ready: ready, authority: authority, sink: sink)
            proxy = server
            let port = try await server.start(port: 0)
            let result = try release022RunProcess(executable: peer.python, arguments: [
                Release022ExternalPeer.clientsDirectory.appendingPathComponent("chromium_client.py").path,
                "--executable",
                executable,
                "--proxy",
                "http://127.0.0.1:\(port)",
                "--url",
                "https://localhost:\(ready.port)/"
            ])
            FileHandle.standardError.write(Data("CLIENT-MATRIX \(result)".utf8))
            XCTAssertTrue(result.contains("\"sse\": true"))
            try assertBrowserCapture(sink)
            try await server.stop()
            try runnerTrust.restore()
            try await group.shutdownGracefully()
        } catch {
            if let proxy {
                try? await proxy.stop()
            }
            try? await group.shutdownGracefully()
            if let trust {
                try trust.restore()
            }
            throw error
        }
    }

    private func makeProxy(
        group: MultiThreadedEventLoopGroup,
        ready: Release022PeerReady,
        authority: CertificateAuthority,
        sink: Release022Events
    ) throws -> ProxyServer {
        ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [try String(contentsOfFile: ready.root, encoding: .utf8)]),
            egressPolicy: .init(allowInternal: true),
            ingress: .explicitHTTP,
            captureBodyLimit: 65_536
        )
    }

    private func assertBrowserCapture(_ sink: Release022Events) throws {
        let events = try sink.wait {
            $0.contains {
                if case .webSocketClose = $0 {
                    true
                } else {
                    false
                }
            }
        }
        let requests = events.compactMap { event -> CapturedRequestHead? in
            if case let .requestHead(head) = event {
                head
            } else {
                nil
            }
        }
        XCTAssertTrue(requests.contains { $0.path == "/events" })
        XCTAssertTrue(requests.contains { $0.path == "/socket" })
        let sseID = try XCTUnwrap(requests.first { $0.path == "/events" }?.id)
        let body = events.flatMap { event -> [UInt8] in
            if case let .responseBodyChunk(id, bytes, _) = event, id == sseID {
                bytes
            } else {
                []
            }
        }
        XCTAssertEqual(body, Array(": heartbeat\n\ndata: €-first\n\ndata: second\n\n".utf8))
        assertBrowserWebSocketCapture(events)
    }

    private func browserConfiguration(
        port: Int,
        authority: CertificateAuthority
    ) throws -> (WKWebViewConfiguration, SecCertificate) {
        let configuration = WKWebViewConfiguration()
        configuration.websiteDataStore = .nonPersistent()
        let endpointPort = try XCTUnwrap(NWEndpoint.Port(rawValue: try XCTUnwrap(UInt16(exactly: port))))
        configuration.websiteDataStore.proxyConfigurations = [
            ProxyConfiguration(httpCONNECTProxy: .hostPort(host: "127.0.0.1", port: endpointPort))
        ]
        let der = try NIOSSLCertificate(bytes: Array(authority.caCertificatePEM.utf8), format: .pem).toDERBytes()
        let root = try XCTUnwrap(SecCertificateCreateWithData(nil, Data(der) as CFData))
        return (configuration, root)
    }

    private func assertBrowserWebSocketCapture(_ events: [CaptureEvent]) {
        XCTAssertEqual(
            events.filter {
            if case .webSocketOpen = $0 {
                true
            } else {
                false
            }
        }
.count,
            1
        )
        XCTAssertEqual(
            events.filter {
            if case .webSocketClose = $0 {
                true
            } else {
                false
            }
        }
.count,
            1
        )
        XCTAssertFalse(events.contains {
            if case .streamError = $0 {
                true
            } else {
                false
            }
        })
    }
}

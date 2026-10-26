import Foundation
import NIOPosix
import XCTest

import SwiftMITM

final class PublicProxyHTTP2WebSocketTests: XCTestCase {
    func testPhase3TLSFixturesPerformDirectRFC8441Exchange() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let origin = try Phase3TLSHTTP2WebSocketOrigin(group: group)
        let client = Phase3ProxyHTTP2WebSocketClient(group: group)
        defer {
            client.stop()
            origin.stop()
        }
        try origin.start()

        let clientResult = try client.exchangeDirectly(
            originHost: origin.hostname,
            originPort: origin.localPort,
            originCACertificatePEM: origin.caCertificatePEM
        )
        let originResult = try origin.result.wait()

        XCTAssertTrue(clientResult.downstreamExtendedConnectEnabled)
        XCTAssertEqual(clientResult.status, 200)
        XCTAssertEqual(clientResult.serverBytes, WebSocketWire.serverFrames)
        XCTAssertEqual(originResult.clientBytes, WebSocketWire.clientFrames)
    }

    func testPublicProxyForwardsAndCapturesRFC8441Exchange() async throws {
        let result = try await performExchange(scenario: .accepted)

        XCTAssertTrue(result.client.downstreamExtendedConnectEnabled)
        XCTAssertEqual(result.client.status, 200)
        XCTAssertEqual(result.client.serverBytes, WebSocketWire.serverFrames)
        XCTAssertEqual(result.origin?.clientBytes, WebSocketWire.clientFrames)
        try assertAcceptedCapture(result.snapshot, authority: result.authority)
    }

    func testPublicProxyDoesNotAdvertiseExtendedConnectWithoutOriginCapability() async throws {
        let result = try await performExchange(
            advertisesExtendedConnect: false,
            scenario: .accepted
        )

        XCTAssertFalse(result.client.downstreamExtendedConnectEnabled)
        XCTAssertNil(result.client.status)
        XCTAssertNil(result.origin)
        XCTAssertTrue(result.snapshot.eventKinds.isEmpty)
    }

    func testFailedRFC8441HandshakeEmitsNoWebSocketLifecycle() async throws {
        let result = try await performExchange(scenario: .finalStatus(403))

        XCTAssertTrue(result.client.downstreamExtendedConnectEnabled)
        XCTAssertEqual(result.client.status, 403)
        XCTAssertEqual(result.origin?.clientBytes, [])
        XCTAssertEqual(result.snapshot.responseHeads.map(\.status), [403])
        XCTAssertTrue(result.snapshot.opens.isEmpty)
        XCTAssertTrue(result.snapshot.frames.isEmpty)
        XCTAssertTrue(result.snapshot.closes.isEmpty)
    }

    func testSuccessfulRFC8441HandshakeEndingWithHeadersEmitsNoWebSocketLifecycle() async throws {
        let result = try await performExchange(scenario: .prematureSuccessEnd)

        XCTAssertTrue(result.client.downstreamExtendedConnectEnabled)
        XCTAssertEqual(result.client.status, 200)
        XCTAssertEqual(result.origin?.clientBytes, [])
        XCTAssertEqual(result.snapshot.responseHeads.map(\.status), [200])
        XCTAssertTrue(result.snapshot.opens.isEmpty)
        XCTAssertTrue(result.snapshot.frames.isEmpty)
        XCTAssertTrue(result.snapshot.closes.isEmpty)
    }

    func testRFC8441StreamResetEmitsOneCloseAndOneStreamError() async throws {
        let snapshot = try await performFailedExchange(scenario: .reset)

        XCTAssertEqual(snapshot.opens.count, 1)
        XCTAssertEqual(snapshot.closes.count, 1)
        XCTAssertEqual(snapshot.eventKinds.filter { $0 == .streamError }.count, 1)
    }

    func testRFC8441ConnectionLossEmitsOneCloseAndOneStreamError() async throws {
        let snapshot = try await performFailedExchange(scenario: .connectionLoss)

        XCTAssertEqual(snapshot.opens.count, 1)
        XCTAssertEqual(snapshot.closes.count, 1)
        XCTAssertEqual(snapshot.eventKinds.filter { $0 == .streamError }.count, 1)
    }

    private func performExchange(
        advertisesExtendedConnect: Bool = true,
        scenario: Phase3HTTP2WebSocketOriginScenario
    ) async throws -> Phase3PublicExchangeResult {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Phase3TLSHTTP2WebSocketOrigin(
            group: group,
            advertisesExtendedConnect: advertisesExtendedConnect,
            scenario: scenario
        )
        let mitmCA = try CertificateAuthority.generate().authority
        let sink = WebSocketRecordingSink()
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 64
        )
        let client = Phase3ProxyHTTP2WebSocketClient(group: group)

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let authority = "\(origin.hostname):\(origin.localPort)"
            let clientResult = try await runBlocking {
                try client.exchange(
                    proxyPort: proxyPort,
                    originHost: origin.hostname,
                    originPort: origin.localPort,
                    mitmCACertificatePEM: mitmCA.caCertificatePEM
                )
            }
            let originResult = advertisesExtendedConnect
                ? try await runBlocking { try origin.result.wait() }
                : nil
            client.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
            return .init(
                authority: authority,
                client: clientResult,
                origin: originResult,
                snapshot: sink.snapshot
            )
        } catch {
            client.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func performFailedExchange(
        scenario: Phase3HTTP2WebSocketOriginScenario
    ) async throws -> WebSocketRecordingSink.Snapshot {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Phase3TLSHTTP2WebSocketOrigin(group: group, scenario: scenario)
        let mitmCA = try CertificateAuthority.generate().authority
        let sink = WebSocketRecordingSink()
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 64
        )
        let client = Phase3ProxyHTTP2WebSocketClient(group: group)

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            var clientFailed = false
            do {
                _ = try await runBlocking {
                    try client.exchange(
                        proxyPort: proxyPort,
                        originHost: origin.hostname,
                        originPort: origin.localPort,
                        mitmCACertificatePEM: mitmCA.caCertificatePEM
                    )
                }
            } catch {
                clientFailed = true
            }
            XCTAssertTrue(clientFailed)
            client.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
            return sink.snapshot
        } catch {
            client.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertAcceptedCapture(
        _ snapshot: WebSocketRecordingSink.Snapshot,
        authority: String
    ) throws {
        XCTAssertEqual(snapshot.requestHeads.count, 1)
        XCTAssertEqual(snapshot.requestHeads.first?.scheme, "https")
        XCTAssertEqual(snapshot.requestHeads.first?.authority, authority)
        XCTAssertEqual(snapshot.requestHeads.first?.method, "CONNECT")
        XCTAssertEqual(snapshot.requestHeads.first?.path, "/socket")
        XCTAssertEqual(snapshot.requestHeads.first?.version, "HTTP/2")
        XCTAssertEqual(snapshot.responseHeads.map(\.status), [200])

        let requestID = try XCTUnwrap(snapshot.requestHeads.first?.id)
        XCTAssertEqual(snapshot.responseHeads.first?.requestID, requestID)
        XCTAssertEqual(snapshot.opens.map(\.connectionID), [requestID])
        XCTAssertEqual(snapshot.frames.map(\.connectionID), Array(repeating: requestID, count: 4))
        XCTAssertEqual(snapshot.frames.map(\.direction), [
            .clientToServer,
            .serverToClient,
            .clientToServer,
            .serverToClient
        ])
        XCTAssertEqual(snapshot.frames.map(\.opcode), [.text, .binary, .connectionClose, .connectionClose])
        XCTAssertEqual(snapshot.frames.map(\.bytes), [
            WebSocketWire.clientTextPayload,
            WebSocketWire.serverBinaryPayload,
            WebSocketWire.closePayload,
            WebSocketWire.closePayload
        ])
        XCTAssertEqual(snapshot.frames.map(\.byteCount), [11, 4, 6, 6])
        XCTAssertEqual(snapshot.frames.map(\.truncated), [false, false, false, false])
        XCTAssertEqual(snapshot.closes.map(\.connectionID), [requestID])
        XCTAssertEqual(snapshot.closes.map(\.code), [1000])
        XCTAssertEqual(snapshot.closes.map(\.reason), ["done"])
        XCTAssertEqual(snapshot.eventKinds, [
            .requestHead,
            .responseHead,
            .webSocketOpen,
            .webSocketFrame(direction: "clientToServer", opcode: 0x1),
            .webSocketFrame(direction: "serverToClient", opcode: 0x2),
            .webSocketFrame(direction: "clientToServer", opcode: 0x8),
            .webSocketClose,
            .webSocketFrame(direction: "serverToClient", opcode: 0x8)
        ])
    }

    private func runBlocking<T: Sendable>(
        _ operation: @escaping @Sendable () throws -> T
    ) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

private struct Phase3PublicExchangeResult {
    let authority: String
    let client: Phase3ProxyHTTP2WebSocketClientResult
    let origin: Phase3HTTP2WebSocketOriginResult?
    let snapshot: WebSocketRecordingSink.Snapshot
}

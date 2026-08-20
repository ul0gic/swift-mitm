import Foundation
import NIOCore
import NIOPosix
import XCTest
import X509

import SwiftMITM

final class PublicProxyWebSocketTests: XCTestCase {
    func testPublicProxyForwardsAndCapturesHTTP1WebSocketExchange() async throws {
        try await assertPublicProxyWebSocketExchange(targetHost: "localhost")
    }

    func testPublicProxyPreservesExplicitIPTargetForHTTP1WebSocketExchange() async throws {
        try await assertPublicProxyWebSocketExchange(targetHost: "127.0.0.1")
    }

    func testRejectedUpgradePreservesSubsequentHTTP1Capture() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try RejectedWebSocketTLSOriginServer(group: group)
        let mitmCA = try CertificateAuthority.generate().authority
        let sink = WebSocketRecordingSink()
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 8
        )

        do {
            try origin.start()
            let originPort = origin.localPort
            let proxyPort = try await proxy.start(port: 0)
            let responseBytes = try await runBlocking {
                try RejectedWebSocketProxyClient(group: group).exchange(
                    proxyPort: proxyPort,
                    originPort: originPort,
                    mitmCACertificatePEM: mitmCA.caCertificatePEM
                )
            }
            let originRequestHeads = try await runBlocking { try origin.waitForRequestHeads() }
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()

            assertRejectedUpgradeExchange(
                originPort: originPort,
                originRequestHeads: originRequestHeads,
                responseBytes: responseBytes,
                snapshot: sink.snapshot
            )
        } catch {
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertRejectedUpgradeExchange(
        originPort: Int,
        originRequestHeads: [[UInt8]],
        responseBytes: [UInt8],
        snapshot: WebSocketRecordingSink.Snapshot
    ) {
        XCTAssertEqual(originRequestHeads, [
            RejectedWebSocketClientHandler.upgradeRequest(originPort: originPort),
            RejectedWebSocketClientHandler.followUpRequest(originPort: originPort)
        ])
        XCTAssertEqual(
            responseBytes,
            RejectedWebSocketClientHandler.rejectionResponse + RejectedWebSocketClientHandler.successResponse
        )
        XCTAssertEqual(snapshot.requestHeads.map(\.path), ["/socket", "/after"])
        XCTAssertEqual(snapshot.responseHeads.map(\.status), [403, 204])
        XCTAssertTrue(snapshot.opens.isEmpty)
        XCTAssertTrue(snapshot.frames.isEmpty)
        XCTAssertTrue(snapshot.closes.isEmpty)
        XCTAssertEqual(snapshot.eventKinds, [
            .requestHead, .requestEnd, .responseHead, .responseBody, .responseEnd,
            .requestHead, .requestEnd, .responseHead, .responseEnd
        ])
    }

    private func assertPublicProxyWebSocketExchange(targetHost: String) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try WebSocketTLSOriginServer(group: group, targetHost: targetHost)
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

        do {
            try origin.start()
            let originPort = origin.localPort
            let proxyPort = try await proxy.start(port: 0)
            let exchange = try await performExchange(
                group: group,
                origin: origin,
                originHost: targetHost,
                originPort: originPort,
                proxyPort: proxyPort,
                mitmCACertificatePEM: mitmCA.caCertificatePEM
            )
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
            try assertExchange(
                exchange,
                originHost: targetHost,
                originPort: originPort,
                snapshot: sink.snapshot
            )
        } catch {
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func performExchange(
        group: EventLoopGroup,
        origin: WebSocketTLSOriginServer,
        originHost: String,
        originPort: Int,
        proxyPort: Int,
        mitmCACertificatePEM: String
    ) async throws -> WebSocketExchange {
        try await runBlocking {
            let clientExchange = try WebSocketProxyClient(group: group).exchange(
                proxyPort: proxyPort,
                originHost: originHost,
                originPort: originPort,
                mitmCACertificatePEM: mitmCACertificatePEM
            )
            let originResult = try origin.waitForResult()
            return WebSocketExchange(
                originRequestHead: originResult.requestHead,
                clientResponseHead: clientExchange.responseHead,
                originReceivedFrames: originResult.frames,
                clientReceivedFrames: clientExchange.frames,
                peerCertificateDER: clientExchange.peerCertificateDER
            )
        }
    }

    private func assertExchange(
        _ exchange: WebSocketExchange,
        originHost: String,
        originPort: Int,
        snapshot: WebSocketRecordingSink.Snapshot
    ) throws {
        assertWireExchange(exchange, originHost: originHost, originPort: originPort)
        let requestID = try assertHandshakeCapture(
            originHost: originHost,
            originPort: originPort,
            snapshot: snapshot
        )
        assertFrameCapture(requestID: requestID, snapshot: snapshot)
        try assertPeerIdentity(exchange.peerCertificateDER, targetHost: originHost)
    }

    private func assertWireExchange(_ exchange: WebSocketExchange, originHost: String, originPort: Int) {
        XCTAssertEqual(
            exchange.originRequestHead,
            WebSocketWire.requestHead(originHost: originHost, originPort: originPort)
        )
        XCTAssertEqual(exchange.clientResponseHead, WebSocketWire.responseHead)
        XCTAssertEqual(exchange.originReceivedFrames, WebSocketWire.clientFrames)
        XCTAssertEqual(exchange.clientReceivedFrames, WebSocketWire.serverFrames)
    }

    private func assertHandshakeCapture(
        originHost: String,
        originPort: Int,
        snapshot: WebSocketRecordingSink.Snapshot
    ) throws -> UUID {
        XCTAssertEqual(snapshot.requestHeads.count, 1)
        XCTAssertEqual(snapshot.requestHeads.first?.scheme, "https")
        XCTAssertEqual(snapshot.requestHeads.first?.authority, "\(originHost):\(originPort)")
        XCTAssertEqual(snapshot.requestHeads.first?.method, "GET")
        XCTAssertEqual(snapshot.requestHeads.first?.path, "/socket")
        XCTAssertEqual(snapshot.requestHeads.first?.version, "HTTP/1.1")
        XCTAssertEqual(snapshot.requestHeads.first?.headers, [
            HTTPHeaderField(name: "Host", value: "\(originHost):\(originPort)"),
            HTTPHeaderField(name: "Upgrade", value: "websocket"),
            HTTPHeaderField(name: "Connection", value: "Upgrade"),
            HTTPHeaderField(name: "Sec-WebSocket-Version", value: "13"),
            HTTPHeaderField(name: "Sec-WebSocket-Key", value: "dGhlIHNhbXBsZSBub25jZQ==")
        ])
        XCTAssertEqual(snapshot.responseHeads.map(\.status), [101])
        XCTAssertEqual(snapshot.responseHeads.first?.version, "HTTP/1.1")
        XCTAssertEqual(snapshot.responseHeads.first?.headers, [
            HTTPHeaderField(name: "Upgrade", value: "websocket"),
            HTTPHeaderField(name: "Connection", value: "Upgrade"),
            HTTPHeaderField(name: "Sec-WebSocket-Accept", value: "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        ])

        let requestID = try XCTUnwrap(snapshot.requestHeads.first?.id)
        XCTAssertEqual(snapshot.responseHeads.first?.requestID, requestID)
        XCTAssertEqual(snapshot.opens.map(\.connectionID), [requestID])
        XCTAssertEqual(snapshot.opens.map(\.permessageDeflate), [false])
        return requestID
    }

    private func assertFrameCapture(requestID: UUID, snapshot: WebSocketRecordingSink.Snapshot) {
        XCTAssertEqual(snapshot.frames.count, 4)
        XCTAssertEqual(snapshot.frames.map(\.connectionID), Array(repeating: requestID, count: 4))
        XCTAssertEqual(snapshot.frames.map(\.direction), [
            .clientToServer,
            .serverToClient,
            .clientToServer,
            .serverToClient
        ])
        XCTAssertEqual(snapshot.frames.map(\.opcode), [.text, .binary, .connectionClose, .connectionClose])
        XCTAssertEqual(snapshot.frames.map(\.fin), [true, true, true, true])
        XCTAssertEqual(snapshot.frames.map(\.compressed), [false, false, false, false])
        XCTAssertEqual(snapshot.frames.map(\.bytes), [
            WebSocketWire.clientTextPayload,
            WebSocketWire.serverBinaryPayload,
            WebSocketWire.closePayload,
            WebSocketWire.closePayload
        ])
        XCTAssertEqual(snapshot.frames.map(\.byteCount), [11, 4, 6, 6])
        XCTAssertEqual(snapshot.frames.map(\.truncated), [false, false, false, false])
        XCTAssertEqual(snapshot.frames.map(\.closeCode), [nil, nil, 1000, 1000])
        XCTAssertEqual(snapshot.frames.map(\.closeReason), [nil, nil, "done", "done"])
        XCTAssertEqual(snapshot.closes.count, 1)
        XCTAssertEqual(snapshot.closes.first?.connectionID, requestID)
        XCTAssertEqual(snapshot.closes.first?.code, 1000)
        XCTAssertEqual(snapshot.closes.first?.reason, "done")
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

    private func assertPeerIdentity(_ derBytes: [UInt8], targetHost: String) throws {
        let certificate = try Certificate(derEncoded: derBytes)
        let names = try XCTUnwrap(certificate.extensions.subjectAlternativeNames)
        if (try? SocketAddress(ipAddress: targetHost, port: 0)) != nil {
            XCTAssertEqual(names.count, 1)
            guard case .ipAddress(let address) = names.first else {
                return XCTFail("intercepted IPv4 leaf must use an IP-address SAN")
            }
            XCTAssertEqual(Array(address.bytes), [127, 0, 0, 1])
            XCTAssertFalse(names.contains(.dnsName(targetHost)))
        } else {
            XCTAssertTrue(names.contains(.dnsName(targetHost)))
        }
    }

    private func runBlocking<T: Sendable>(_ operation: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicReadOwnershipTests: XCTestCase {
    func testExplicitHTTPReadsClientHelloSentAfterCONNECTResponse() async throws {
        try await release022AssertIdentity(
            originName: "localhost",
            serverName: "localhost",
            succeeds: true,
            ingress: .explicitHTTP
        )
    }

    func testExplicitWebSocketReadsLaterFramesAfterEachDirectionBarrier() async throws {
        try await release022WithWire { origin, client, received, sink in
            let request = "GET http://127.0.0.1:\(origin.port)/socket HTTP/1.1\r\n"
                + "Host: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
                + "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            let upstreamHead = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try origin.write(WebSocketWire.responseHead)
            let downstreamHead = try received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            _ = try sink.wait { $0.contains(where: self.isWebSocketOpen) }
            for count in 1 ... 2 {
                try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: WebSocketWire.clientTextFrame)))
                let bytes = try origin.received.waitForBytes(
                    upstreamHead.count + count * WebSocketWire.clientTextFrame.count
                )
                XCTAssertEqual(
                    Array(bytes.suffix(WebSocketWire.clientTextFrame.count)),
                    WebSocketWire.clientTextFrame
                )
                _ = try sink.wait { self.frames($0).count == count }
            }
            for count in 1 ... 2 {
                try origin.write(WebSocketWire.serverBinaryFrame)
                let bytes = try received.waitForBytes(
                    downstreamHead.count + count * WebSocketWire.serverBinaryFrame.count
                )
                XCTAssertEqual(
                    Array(bytes.suffix(WebSocketWire.serverBinaryFrame.count)),
                    WebSocketWire.serverBinaryFrame
                )
                _ = try sink.wait { self.frames($0).count == 2 + count }
            }
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: WebSocketWire.clientCloseFrame)))
            _ = try sink.wait { self.frames($0).count == 5 }
            XCTAssertFalse(sink.events.contains {
                if case .webSocketClose = $0 {
                    true
                } else {
                    false
                }
            })
            try origin.write(WebSocketWire.serverCloseFrame)
            let events = try sink.wait { $0.contains {
                if case .webSocketClose = $0 {
                    true
                } else {
                    false
                }
            } }
            assertWebSocketDirectionsAndTerminal(events)
        }
    }

    func testLocalStopFailsHeldCloseDelimitedResponseWithoutCleanEnd() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = Release022WireOrigin(group: group)
        let sink = Release022Events()
        let proxy = try readOwnershipProxy(group: group, sink: sink)
        var client: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let received = Release022WireObservation()
            let channel = try release022WireClient(group: group, port: port, observation: received)
            client = channel
            let header = Phase4ProxyV2Header.ipv4(
                source: [192, 0, 2, 1],
                destination: [127, 0, 0, 1],
                sourcePort: 50_002,
                destinationPort: origin.port
            )
            let request = Array("GET /held HTTP/1.1\r\nHost: localhost\r\n\r\n".utf8)
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: header.bytes + request)))
            _ = try origin.received.waitForBytes(request.count)
            let response = Array("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabc".utf8)
            try origin.write(response)
            XCTAssertEqual(try received.waitForBytes(response.count), response)
            _ = try sink.wait { release022ResponseBodyBytes($0) == Array("abc".utf8) }
            XCTAssertTrue(release022ResponseEnds(sink.events).isEmpty)
            try await proxy.stop()
            try received.waitForClose()
            try origin.received.waitForClose()
            assertStoppedResponse(sink.events)
            if channel.isActive {
                try await channel.close().get()
            }
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func frames(_ events: [CaptureEvent]) -> [CapturedWebSocketFrame] {
        events.compactMap {
            if case let .webSocketFrame(frame) = $0 {
                frame
            } else {
                nil
            }
        }
    }

    private func assertWebSocketDirectionsAndTerminal(_ events: [CaptureEvent]) {
        XCTAssertEqual(self.frames(events).map(\.direction), [
            .clientToServer,
            .clientToServer,
            .serverToClient,
            .serverToClient,
            .clientToServer,
            .serverToClient
        ])
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
        XCTAssertTrue(release022Errors(events).isEmpty)
    }

    private func assertStoppedResponse(_ events: [CaptureEvent]) {
        XCTAssertEqual(release022Requests(events).map(\.path), ["/held"])
        let requestEnds = events.compactMap { event -> UUID? in
            if case let .requestEnd(id, _) = event {
                id
            } else {
                nil
            }
        }
        XCTAssertEqual(requestEnds, release022Requests(events).map(\.id))
        XCTAssertEqual(release022Errors(events), release022Requests(events).map(\.id))
        XCTAssertTrue(release022ResponseEnds(events).isEmpty)
    }

    private func isWebSocketOpen(_ event: CaptureEvent) -> Bool {
        if case .webSocketOpen = event {
            true
        } else {
            false
        }
    }

    private func readOwnershipProxy(group: EventLoopGroup, sink: Release022Events) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        return ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            captureBodyLimit: 8
        )
    }
}

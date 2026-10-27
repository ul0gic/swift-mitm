import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicHTTP1ForwardingTests: XCTestCase {
    func testAbsoluteURIUsesTargetOriginAndStripsHopFields() async throws {
        try await release022WithWire { origin, client, received, sink in
            let request = "GET http://127.0.0.1:\(origin.port)/target?q=1 HTTP/1.1\r\n"
                + "Host: attacker.invalid\r\nConnection: x-secret\r\nConnection: keep-alive\r\n"
                + "X-Secret: remove\r\nProxy-Authorization: Basic c2VjcmV0\r\nX-End: retain\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            let bytes = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let wire = try XCTUnwrap(String(bytes: bytes, encoding: .utf8)).lowercased()
            XCTAssertTrue(wire.hasPrefix("get /target?q=1 http/1.1\r\n"))
            XCTAssertTrue(wire.contains("host: 127.0.0.1:\(origin.port)\r\n"))
            XCTAssertTrue(wire.contains("x-end: retain\r\n"))
            XCTAssertTrue(wire.contains("via:"))
            XCTAssertFalse(wire.contains("x-secret:"))
            XCTAssertFalse(wire.contains("proxy-authorization:"))
            try origin.write(Array("HTTP/1.1 204 No Content\r\n\r\n".utf8))
            _ = try received.wait { String(bytes: $0, encoding: .utf8)?.contains("204") == true }
            let events = try sink.wait { release022ResponseEnds($0).count == 1 }
            let host = release022Requests(events).first?.headers.first { $0.name.lowercased() == "host" }
            XCTAssertEqual(host?.value, "attacker.invalid")
        }
    }

    func testAbsoluteServerWideOPTIONSBecomesAsteriskForm() async throws {
        try await release022WithWire { origin, client, _, sink in
            let request = "OPTIONS http://127.0.0.1:\(origin.port) HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            let wire = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            XCTAssertTrue(try XCTUnwrap(String(bytes: wire, encoding: .utf8)).hasPrefix("OPTIONS * HTTP/1.1\r\n"))
            try origin.write(Array("HTTP/1.1 204 No Content\r\n\r\n".utf8))
            _ = try sink.wait { release022ResponseEnds($0).count == 1 }
        }
    }

    func testAmbiguousFramingRejectedBeforeOriginForwarding() async throws {
        try await release022WithWire { origin, client, received, _ in
            let request = "POST http://127.0.0.1:\(origin.port)/ HTTP/1.1\r\nHost: localhost\r\n"
                + "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            try received.waitForClose()
            XCTAssertTrue(try XCTUnwrap(String(bytes: received.bytes, encoding: .utf8)).hasPrefix("HTTP/1.1 400"))
            XCTAssertTrue(origin.received.bytes.isEmpty)
            XCTAssertEqual(origin.acceptedConnections, 0)
        }
    }

    func testExpectContinueMakesProgressBeforeClientSendsBody() async throws {
        try await release022WithWire { origin, client, received, sink in
            let request = "POST http://127.0.0.1:\(origin.port)/upload HTTP/1.1\r\nHost: localhost\r\n"
                + "Content-Length: 3\r\nExpect: 100-continue\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            let head = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try origin.write(Array("HTTP/1.1 100 Continue\r\n\r\n".utf8))
            _ = try received.wait { String(bytes: $0, encoding: .utf8)?.contains("100 Continue") == true }
            XCTAssertFalse(sink.events.contains {
                if case .requestEnd = $0 {
                    true
                } else {
                    false
                }
            })
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: "abc")))
            let wire = try origin.received.waitForBytes(head.count + 3)
            XCTAssertEqual(Array(wire.suffix(3)), Array("abc".utf8))
            try origin.write(Array("HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n".utf8))
            _ = try sink.wait { release022ResponseEnds($0).count == 1 }
            let statuses = sink.events.compactMap { event -> Int? in
                if case let .responseHead(head) = event {
                    head.status
                } else {
                    nil
                }
            }
            XCTAssertEqual(statuses, [100, 201])
        }
    }

    func testEarlyFinalResponseDoesNotWaitForUnsentExpectBody() async throws {
        try await release022WithWire { origin, client, received, sink in
            let request = "POST http://127.0.0.1:\(origin.port)/denied HTTP/1.1\r\nHost: localhost\r\n"
                + "Content-Length: 100\r\nExpect: 100-continue\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            _ = try origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let response = "HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            try origin.write(Array(response.utf8))
            _ = try received.wait { String(bytes: $0, encoding: .utf8)?.contains("413") == true }
            let events = try sink.wait { release022ResponseEnds($0).count == 1 }
            XCTAssertEqual(release022Requests(events).count, 1)
            XCTAssertFalse(events.contains {
                if case .requestEnd = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    func testPipelinedCrossOriginRequestsKeepResponseOrderAndSeparateConnections() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let first = Release022WireOrigin(group: group)
        let second = Release022WireOrigin(group: group)
        let sink = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: try CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            egressPolicy: .init(allowInternal: true),
            ingress: .explicitHTTP
        )
        var client: Channel?
        do {
            try first.start()
            try second.start()
            let port = try await proxy.start(port: 0)
            let received = Release022WireObservation()
            let channel = try release022WireClient(group: group, port: port, observation: received)
            client = channel
            let requests = "GET http://127.0.0.1:\(first.port)/one HTTP/1.1\r\nHost: ignored\r\n\r\n"
                + "GET http://127.0.0.1:\(second.port)/two HTTP/1.1\r\nHost: ignored\r\n\r\n"
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(string: requests)))
            let firstRequest = try first.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            XCTAssertTrue(try XCTUnwrap(String(bytes: firstRequest, encoding: .utf8))
                .hasPrefix("GET /one HTTP/1.1\r\n"))
            XCTAssertTrue(second.received.bytes.isEmpty)
            try first.write(Array("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nA".utf8))
            let secondRequest = try second.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            XCTAssertTrue(try XCTUnwrap(String(bytes: secondRequest, encoding: .utf8))
                .hasPrefix("GET /two HTTP/1.1\r\n"))
            XCTAssertEqual(first.received.bytes, firstRequest)
            try second.write(Array("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nB".utf8))
            try assertPipelinedResponseOrder(sink: sink, received: received)
            try await channel.close().get()
            try await proxy.stop()
            first.stop()
            second.stop()
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            try? await proxy.stop()
            first.stop()
            second.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertPipelinedResponseOrder(sink: Release022Events, received: Release022WireObservation) throws {
        let events = try sink.wait { release022ResponseEnds($0).count == 2 }
        XCTAssertEqual(release022Requests(events).map(\.path), ["/one", "/two"])
        XCTAssertEqual(release022ResponseEnds(events), release022Requests(events).map(\.id))
        let wire = try received.wait { String(bytes: $0, encoding: .utf8)?.hasSuffix("\r\n\r\nB") == true }
        XCTAssertTrue(try XCTUnwrap(String(bytes: wire, encoding: .utf8)).contains("\r\n\r\nAHTTP/1.1 200"))
    }

    func testConflictingLengthsAreRejectedBeforeOriginForwarding() async throws {
        try await release022WithWire { origin, client, received, _ in
            let request = "POST http://127.0.0.1:\(origin.port)/ HTTP/1.1\r\nHost: localhost\r\n"
                + "Content-Length: 1\r\nContent-Length: 2\r\n\r\nz"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            try received.waitForClose()
            XCTAssertTrue(try XCTUnwrap(String(bytes: received.bytes, encoding: .utf8)).hasPrefix("HTTP/1.1 400"))
            XCTAssertEqual(origin.acceptedConnections, 0)
        }
    }

    func testAbsoluteRequestTargetCannotBypassEgressPolicy() async throws {
        try await release022WithWire(egressPolicy: .default) { origin, client, received, sink in
            let request = "GET http://127.0.0.1:\(origin.port)/private HTTP/1.1\r\nHost: public.example\r\n\r\n"
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
            try received.waitForClose()
            XCTAssertEqual(origin.acceptedConnections, 0)
            let failures = sink.events.compactMap { event -> CapturedConnectionFailure? in
                if case let .connectionFailure(failure) = event {
                    failure
                } else {
                    nil
                }
            }
            XCTAssertEqual(failures.count, 1)
            XCTAssertEqual(failures.first?.code, .egressDenied)
        }
    }
}

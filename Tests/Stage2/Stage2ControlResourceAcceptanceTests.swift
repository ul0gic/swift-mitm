import Foundation
import NIOCore
import XCTest

import SwiftMITM
import SwiftMITMInspection

final class Stage2ControlResourceAcceptanceTests: XCTestCase {
    func testBodyReplacementAcceptsLimitAndRejectsLimitPlusOneBeforeEgress() async throws {
        for size in [4, 5] {
            let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
                HTTPRule(
                    id: "body",
                    match: HTTPRuleMatch(phase: .request, path: "/replace"),
                    action: .replaceBody(
                        Data("edited".utf8),
                        maximumInputBytes: 4,
                        contentEncoding: nil,
                        overrideNoTransform: false
                    )
                )
            ], limits: HTTPControlLimits()))
            let events = Stage2ControlEvents()
            try await stage2WithHTTPOrigin(runtime: runtime, controlSink: events) { peer in
                let received = Release022WireObservation()
                let client = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
                let head = "POST http://127.0.0.1:\(peer.origin.port)/replace HTTP/1.1\r\nHost: localhost\r\n"
                    + "Content-Length: \(size)\r\n\r\n"
                try phase4BoundedWait(client.writeAndFlush(ByteBuffer(bytes: Array(head.utf8) + [UInt8](
                    repeating: 97,
                    count: size
                ))))
                if size == 4 {
                    try await stage2Await { peer.origin.requests.count == 1 }
                    let request = try XCTUnwrap(peer.origin.requests.first)
                    XCTAssertEqual(request.body, Array("edited".utf8))
                    XCTAssertEqual(request.head.headers.first(name: "content-length"), "6")
                    _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
                } else {
                    try received.waitForClose()
                    XCTAssertTrue(peer.origin.requests.isEmpty)
                    XCTAssertTrue(events.events.contains { event in
                        if case .failure(_, .bodyLimitExceeded) = event {
                            true
                        } else {
                            false
                        }
                    })
                }
                if client.isActive {
                    try await client.close().get()
                }
            }
            let usage = await runtime.resourceUsage()
            XCTAssertEqual(usage.retainedBytes, 0)
        }
    }

    func testMapRemoteStripsCredentialsAndReauthorizesDeniedDestination() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(
            rules: [],
            limits: HTTPControlLimits()
        ))
        try await stage2WithHTTPOrigin(runtime: runtime) { peer in
            let second = Stage2HTTPOrigin(group: peer.group)
            try await second.start()
            do {
                try await runtime.replaceConfiguration(HTTPControlConfiguration(rules: [
                    HTTPRule(
                        id: "route",
                        match: HTTPRuleMatch(phase: .request, path: "/map"),
                        action: .route(authority: "127.0.0.1:\(second.port)", sensitiveHeaders: ["X-Private"])
                    )
                ], limits: HTTPControlLimits()))
                let received = Release022WireObservation()
                let client = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
                let request = "GET http://127.0.0.1:\(peer.origin.port)/map HTTP/1.1\r\nHost: localhost\r\n"
                    + "Authorization: Bearer synthetic-origin-secret\r\nCookie: synthetic-cookie\r\n"
                    + "X-Private: synthetic-custom\r\n\r\n"
                try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
                try await stage2Await { second.requests.count == 1 }
                let mapped = try XCTUnwrap(second.requests.first)
                XCTAssertNil(mapped.head.headers.first(name: "authorization"))
                XCTAssertNil(mapped.head.headers.first(name: "cookie"))
                XCTAssertNil(mapped.head.headers.first(name: "x-private"))
                XCTAssertEqual(mapped.head.headers.first(name: "host"), "127.0.0.1:\(second.port)")
                XCTAssertTrue(peer.origin.requests.isEmpty)
                _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
                try await client.close().get()
                try await second.stop()
            } catch {
                let failure = error
                do { try await second.stop() } catch { XCTFail("Mapped origin teardown failed: \(error)") }
                throw failure
            }
        }
        try await assertMappedDestinationIsReauthorized()
    }

    func testOneHundredEligibleExchangesReuseOnlyTheirDownstreamSession() async throws {
        try await stage2WithHTTPOrigin(reuse: HTTP1ReusePolicy()) { peer in
            let received = Release022WireObservation()
            let client = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
            for index in 0..<100 {
                let request = "GET http://127.0.0.1:\(peer.origin.port)/reuse/\(index) HTTP/1.1\r\n"
                    + "Host: localhost\r\n\r\n"
                try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string: request)))
                _ = try peer.raw.wait { release022ResponseEnds($0).count == index + 1 }
            }
            XCTAssertEqual(peer.origin.requests.count, 100)
            XCTAssertEqual(peer.origin.acceptedConnections, 1)
            let secondReceived = Release022WireObservation()
            let second = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: secondReceived)
            try phase4BoundedWait(second.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/other-session HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 101 }
            XCTAssertEqual(peer.origin.acceptedConnections, 2)
            XCTAssertEqual(peer.origin.requests.last?.head.uri, "/other-session")
            try await second.close().get()
            try await client.close().get()
        }
    }

    func testAdmissionAtConnectionCeilingRejectsNewWorkWithoutEvictingActiveClient() async throws {
        let admission =
            ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration(maximumConnections: 2))
        try await stage2WithHTTPOrigin(admission: admission, reuse: HTTP1ReusePolicy()) { peer in
            let received = Release022WireObservation()
            let first = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
            try phase4BoundedWait(first.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/one HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            XCTAssertEqual(admission.usage.connections, 2)
            let rejected = Release022WireObservation()
            let second = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: rejected)
            try rejected.waitForClose()
            XCTAssertTrue(first.isActive)
            XCTAssertEqual(peer.origin.acceptedConnections, 1)
            try phase4BoundedWait(first.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/two HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 2 }
            XCTAssertEqual(peer.origin.requests.map(\.head.uri), ["/one", "/two"])
            if second.isActive {
                try await second.close().get()
            }
            try await first.close().get()
        }
        XCTAssertEqual(admission.usage.connections, 0)
        XCTAssertEqual(admission.usage.streams, 0)
        XCTAssertEqual(admission.usage.retainedBytes, 0)
    }

    func testDelayedRequestDoesNotBlockIndependentRequest() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "delay",
                match: HTTPRuleMatch(phase: .request, path: "/delayed"),
                action: .fault(HTTPFault(direction: .request, delayMilliseconds: 1000))
            )
        ], limits: HTTPControlLimits()))
        try await stage2WithHTTPOrigin(runtime: runtime) { peer in
            let delayedBytes = Release022WireObservation()
            let delayed = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: delayedBytes)
            let immediateBytes = Release022WireObservation()
            let immediate = try release022WireClient(
                group: peer.group,
                port: peer.proxyPort,
                observation: immediateBytes
            )
            let clock = ContinuousClock()
            let started = clock.now
            try phase4BoundedWait(delayed.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/delayed HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            try phase4BoundedWait(immediate.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/immediate HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try immediateBytes.wait { $0.suffix(4) == [13, 10, 13, 10] }
            XCTAssertTrue(delayedBytes.bytes.isEmpty)
            XCTAssertEqual(peer.origin.requests.first?.head.uri, "/immediate")
            try await stage2Await { peer.origin.requests.count == 2 }
            let elapsed = stage2Milliseconds(started.duration(to: clock.now))
            XCTAssertGreaterThanOrEqual(elapsed, 1000)
            XCTAssertLessThanOrEqual(elapsed, 5000)
            _ = try delayedBytes.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try await delayed.close().get()
            try await immediate.close().get()
        }
    }
}

extension Stage2ControlResourceAcceptanceTests {
    func testTwentyTerminalAndRejectedUpgradeCyclesReleaseInspectionState() async throws {
        let budget = try InspectionMemoryBudget()
        let configuration = try InspectionConfiguration(budget: budget, maximumActiveBodies: 2)
        try await stage2WithHTTPOrigin(inspectorConfiguration: configuration) { peer in
            for index in 0..<20 {
                let received = Release022WireObservation()
                let client = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
                let upgrade = index.isMultiple(of: 2)
                    ? "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
                        + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                    : ""
                try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string:
                    "GET http://127.0.0.1:\(peer.origin.port)/cycle/\(index) HTTP/1.1\r\n"
                        + "Host: localhost\r\n\(upgrade)\r\n")))
                _ = try peer.raw.wait { release022ResponseEnds($0).count == index + 1 }
                let drained = await peer.inspector.drain()
                XCTAssertTrue(drained.drained)
                XCTAssertFalse(peer.inspected.events.contains { event in
                    switch event.payload {
                    case .diagnostic(.capacityExceeded), .diagnostic(.incompleteTransport): true
                    default: false
                    }
                })
                XCTAssertEqual(
                    peer.inspected
                        .events
                        .filter { event in
                            if case .end = event.payload, event.direction == .response {
                                true
                            } else {
                                false
                            }
                        }
                        .count,
                    index + 1
                )
                if client.isActive {
                    try await client.close().get()
                }
            }
            XCTAssertEqual(peer.origin.requests.count, 20)
        }
        XCTAssertEqual(budget.retainedBytes, 0)
    }

    func testReplayUsesControlsAndAcceptsEarlyFinalResponseWithoutRetry() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "replay-header",
                match: HTTPRuleMatch(phase: .request, path: "/early"),
                action: .editHeaders(remove: [], append: [HTTPHeaderField(name: "X-Controlled", value: "yes")])
            )
        ], limits: HTTPControlLimits()))
        try await stage2WithHTTPOrigin(runtime: runtime, reply: { _ in
            Stage2OriginReply(status: .payloadTooLarge, beforeRequestEnd: true)
        }, body: { peer in
            let request = ReplayRequest(
                source: ReplaySource(requestID: UUID(), representation: .original),
                destination: try XCTUnwrap(URL(string: "http://127.0.0.1:\(peer.origin.port)/early")),
                method: "POST",
                headers: [],
                body: [UInt8](repeating: 97, count: 1_048_576),
                bodyIsComplete: true,
                useControls: true
            )
            let result = try await peer.proxy.replay(request, responseSink: Stage2ReplayEvents())
            XCTAssertEqual(result.status, 413)
            XCTAssertEqual(peer.origin.acceptedConnections, 1)
            XCTAssertEqual(peer.origin.requests.count, 1)
            XCTAssertEqual(peer.origin.requests.first?.head.headers.first(name: "x-controlled"), "yes")
        })
    }

    func testResponseBodyReplacementPreservesTrailersAndOriginalCapture() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "response-body",
                match: HTTPRuleMatch(phase: .response, path: "/response"),
                action: .replaceBody(
                    Data("changed".utf8),
                    maximumInputBytes: 4,
                    contentEncoding: nil,
                    overrideNoTransform: false
                )
            )
        ], limits: HTTPControlLimits()))
        let controls = Stage2ControlEvents()
        try await stage2WithInspectionWire(controlRuntime: runtime, controlEventSink: controls) { peer in
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/response HTTP/1.1\r\nHost: localhost\r\n\r\n")))
            _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            try peer.origin.write(Array((
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n"
                    + "ETag: \"old\"\r\n\r\n4\r\norig\r\n0\r\nX-End: complete\r\n\r\n"
            ).utf8))
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            let response = try peer.received.wait { String(bytes: $0, encoding: .utf8)?.contains("X-End: complete") == true }
            let text = try XCTUnwrap(String(bytes: response, encoding: .utf8))
            XCTAssertTrue(text.contains("changed"))
            XCTAssertFalse(text.contains("orig"))
            XCTAssertFalse(text.lowercased().contains("etag:"))
            XCTAssertEqual(stage2ResponseCount(peer.raw.events), 4)
            XCTAssertTrue(peer.raw.events.contains { event in
                if case let .responseTrailers(_, headers) = event {
                    headers.contains { $0.name.lowercased() == "x-end" && $0.value == "complete" }
                } else {
                    false
                }
            })
        }
    }
    private func assertMappedDestinationIsReauthorized() async throws {
        let deniedRuntime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(
            rules: [],
            limits: HTTPControlLimits()
        ))
        let controls = Stage2ControlEvents()
        try await stage2WithHTTPOrigin(runtime: deniedRuntime, controlSink: controls, egressPolicy: .default) { peer in
            try await deniedRuntime.replaceConfiguration(HTTPControlConfiguration(rules: [
                HTTPRule(
                    id: "deny-map",
                    match: HTTPRuleMatch(phase: .request, path: "/map"),
                    action: .route(authority: "127.0.0.1:\(peer.origin.port)", sensitiveHeaders: [])
                )
            ], limits: HTTPControlLimits()))
            let received = Release022WireObservation()
            let client = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string:
                "GET http://reserved.invalid:80/map HTTP/1.1\r\nHost: reserved.invalid\r\n\r\n")))
            try received.waitForClose()
            XCTAssertEqual(peer.origin.acceptedConnections, 0)
            XCTAssertTrue(peer.raw.events.contains { event in
                if case let .connectionFailure(failure) = event {
                    failure.code == .egressDenied
                } else {
                    false
                }
            })
        }
    }
}

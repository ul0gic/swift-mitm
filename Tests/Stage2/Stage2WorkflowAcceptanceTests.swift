import Foundation
import NIOConcurrencyHelpers
import NIOCore
import XCTest

import SwiftMITM

final class Stage2ControlEvents: HTTPControlEventSink {
    private let storage = NIOLockedValueBox<[HTTPControlEvent]>([])
    var events: [HTTPControlEvent] { storage.withLockedValue { $0 } }
    func receive(_ event: HTTPControlEvent) { storage.withLockedValue { $0.append(event) } }

    func breakpoint() async throws -> BreakpointHandle {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(5))
        while true {
            if let handle = events.compactMap({ event -> BreakpointHandle? in
                if case let .breakpoint(handle) = event {
                    handle
                } else {
                    nil
                }
            })
            .first {
                return handle
            }
            guard clock.now < deadline else { throw Phase2FixtureError.deadlineExceeded }
            try await Task.sleep(for: .milliseconds(10))
        }
    }
}

final class Stage2ReplayEvents: ReplayResponseSink {
    private let storage = NIOLockedValueBox<[ReplayResponseEvent]>([])
    var events: [ReplayResponseEvent] { storage.withLockedValue { $0 } }
    func receive(_ event: ReplayResponseEvent) async throws { storage.withLockedValue { $0.append(event) } }
}

final class Stage2HARBytes: HAROutputWriter {
    private let storage = NIOLockedValueBox<[UInt8]>([])
    var bytes: [UInt8] { storage.withLockedValue { $0 } }
    func write(_ bytes: [UInt8]) async throws {
        XCTAssertLessThanOrEqual(bytes.count, 1_048_576)
        storage.withLockedValue { $0.append(contentsOf: bytes) }
    }
}

private struct Stage2HARDocument: Decodable {
    struct Log: Decodable {
        struct Entry: Decodable {
            struct Request: Decodable { let method: String; let url: String }
            struct Response: Decodable { let status: Int }
            let request: Request
            let response: Response
        }
        let version: String
        let entries: [Entry]
    }
    let log: Log
}

final class Stage2WorkflowAcceptanceTests: XCTestCase {
    private struct EffectiveRequest {
        let id: UUID
        let headers: [HTTPHeaderField]
        let rules: [String]
    }

    func testHeaderEditChangesOriginWithoutOverwritingOriginalEvidence() async throws {
        let configuration = try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "replace-probe",
                match: HTTPRuleMatch(phase: .request, path: "/controlled"),
                action: .editHeaders(
                    remove: ["X-Probe"],
                    append: [HTTPHeaderField(name: "X-Probe", value: "effective")]
                )
            )
        ], limits: HTTPControlLimits())
        let controls = Stage2ControlEvents()
        try await stage2WithInspectionWire(
            controlRuntime: HTTPControlRuntime(configuration: configuration),
            controlEventSink: controls
        ) { peer in
            let request = "GET http://127.0.0.1:\(peer.origin.port)/controlled HTTP/1.1\r\n"
                + "Host: localhost\r\nX-Probe: original\r\n\r\n"
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
            let forwarded = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let wire = try XCTUnwrap(String(bytes: forwarded, encoding: .utf8))
            XCTAssertTrue(wire.contains("X-Probe: effective\r\n"))
            XCTAssertFalse(wire.contains("X-Probe: original\r\n"))
            try peer.origin.write(Array("HTTP/1.1 204 No Content\r\n\r\n".utf8))
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            let original = try XCTUnwrap(release022Requests(peer.raw.events).first)
            XCTAssertEqual(original.headers.first { $0.name.lowercased() == "x-probe" }?.value, "original")
            let effective = controls.events.compactMap { event -> EffectiveRequest? in
                if case let .effectiveHead(id, .request, headers, rules) = event {
                    EffectiveRequest(id: id, headers: headers, rules: rules)
                } else {
                    nil
                }
            }
            XCTAssertEqual(effective.count, 1)
            XCTAssertEqual(effective.first?.id, original.id)
            XCTAssertEqual(effective.first?.headers.first { $0.name.lowercased() == "x-probe" }?.value, "effective")
            XCTAssertEqual(effective.first?.rules, ["replace-probe"])
        }
    }

    func testBreakpointResumesOnceAndReleasesItsReservation() async throws {
        let configuration = try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "hold",
                match: HTTPRuleMatch(phase: .request, path: "/pause"),
                action: .breakpoint
            )
        ], limits: HTTPControlLimits())
        let runtime = HTTPControlRuntime(configuration: configuration)
        let controls = Stage2ControlEvents()
        try await stage2WithInspectionWire(controlRuntime: runtime, controlEventSink: controls) { peer in
            let request = "GET http://127.0.0.1:\(peer.origin.port)/pause HTTP/1.1\r\nHost: localhost\r\n\r\n"
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
            let handle = try await controls.breakpoint()
            XCTAssertTrue(peer.origin.received.bytes.isEmpty)
            let first = await handle.resolve(.forwardOriginal)
            if case .resumed = first {} else {
                XCTFail("First resume did not win")
            }
            let duplicate = await handle.resolve(.reject)
            if case .alreadyCompleted = duplicate {} else {
                XCTFail("Duplicate resume changed a completed hold")
            }
            let forwarded = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            XCTAssertTrue(try XCTUnwrap(String(bytes: forwarded, encoding: .utf8)).hasPrefix("GET /pause HTTP/1.1\r\n"))
            try peer.origin.write(Array("HTTP/1.1 204 No Content\r\n\r\n".utf8))
            _ = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
        }
        let usage = await runtime.resourceUsage()
        XCTAssertEqual(usage.pausedExchanges, 0)
        XCTAssertEqual(usage.orphanedCallbacks, 0)
        XCTAssertEqual(usage.retainedBytes, 0)
    }

    func testReplaySendsOneCredentialFreePOSTAndRejectsIncompleteInputBeforeConnect() async throws {
        try await stage2WithInspectionWire { peer in
            let destination = try XCTUnwrap(URL(string: "http://127.0.0.1:\(peer.origin.port)/replay"))
            let sourceID = UUID()
            let request = ReplayRequest(
                source: ReplaySource(requestID: sourceID, representation: .original),
                destination: destination,
                method: "POST",
                headers: [
                    HTTPHeaderField(name: "Authorization", value: "Bearer synthetic-replay-secret"),
                    HTTPHeaderField(name: "Cookie", value: "session=synthetic-cookie-secret"),
                    HTTPHeaderField(name: "Content-Type", value: "text/plain")
                ],
                body: Array("payload".utf8),
                bodyIsComplete: true
            )
            let proxy = peer.proxy
            let replayEvents = Stage2ReplayEvents()
            async let replay = proxy.replay(request, responseSink: replayEvents)
            let bytes = try peer.origin.received.wait { $0.suffix(7).elementsEqual(Array("payload".utf8)) }
            let wire = try XCTUnwrap(String(bytes: bytes, encoding: .utf8))
            XCTAssertTrue(wire.hasPrefix("POST /replay HTTP/1.1\r\n"))
            XCTAssertFalse(wire.lowercased().contains("authorization:"))
            XCTAssertFalse(wire.lowercased().contains("cookie:"))
            try peer.origin.write(Array("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".utf8))
            let result = try await replay
            XCTAssertEqual(result.sourceRequestID, sourceID)
            XCTAssertNotEqual(result.requestID, sourceID)
            XCTAssertEqual(result.status, 200)
            XCTAssertEqual(result.responseBodyBytes, 2)
            XCTAssertEqual(peer.origin.acceptedConnections, 1)
            XCTAssertEqual(
                replayEvents.events
                    .filter {
                        if case .end = $0 {
                            true
                        } else {
                            false
                        }
                    }
                    .count,
                1
            )
            try await assertIncompleteReplay(
                peer: peer,
                destination: destination,
                sourceID: sourceID,
                replayEvents: replayEvents,
                bytes: bytes
            )
        }
    }

    func testHARExportRedactsEveryCapturedCredentialLocationAndOmitsBody() async throws {
        try await stage2WithInspectionWire { peer in
            let request = "GET http://127.0.0.1:\(peer.origin.port)/export"
                + "?token=synthetic-query-secret&keep=yes HTTP/1.1\r\n"
                + "Host: localhost\r\nAuthorization: Bearer synthetic-header-secret\r\n"
                + "Cookie: session=synthetic-cookie-secret\r\n\r\n"
            let clock = ContinuousClock()
            let sendStart = clock.now
            try phase4BoundedWait(peer.client.writeAndFlush(ByteBuffer(string: request)))
            _ = try peer.origin.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let sendEnd = clock.now
            let body = Array("synthetic-body-secret".utf8)
            try peer.origin.write(Array((
                "HTTP/1.1 200 OK\r\nSet-Cookie: session=synthetic-response-secret\r\n"
                    + "Content-Type: text/plain\r\nContent-Length: \(body.count)\r\n\r\n"
            ).utf8))
            _ = try peer.received.wait { $0.suffix(4) == [13, 10, 13, 10] }
            let firstResponse = clock.now
            try peer.origin.write(body)
            let captured = try peer.raw.wait { release022ResponseEnds($0).count == 1 }
            _ = try peer.received.wait { $0.suffix(body.count).elementsEqual(body) }
            let responseEnd = clock.now
            let timings = try HARExchangeTiming(
                send: stage2Milliseconds(sendStart.duration(to: sendEnd)),
                wait: stage2Milliseconds(sendEnd.duration(to: firstResponse)),
                receive: stage2Milliseconds(firstResponse.duration(to: responseEnd))
            )
            try await assertHARExport(captured: captured, body: body, timings: timings)
        }
    }

    private func assertIncompleteReplay(
        peer: Stage2WireContext,
        destination: URL,
        sourceID: UUID,
        replayEvents: Stage2ReplayEvents,
        bytes: [UInt8]
    ) async throws {
        let incomplete = ReplayRequest(
            source: ReplaySource(requestID: sourceID, representation: .original),
            destination: destination,
            method: "POST",
            headers: [],
            body: [1],
            bodyIsComplete: false
        )
        do {
            _ = try await peer.proxy.replay(incomplete, responseSink: replayEvents)
            XCTFail("Truncated input reached replay")
        } catch ReplayError.incompleteBody {} catch { throw error }
        XCTAssertEqual(peer.origin.acceptedConnections, 1)
        XCTAssertEqual(peer.origin.received.bytes, bytes)
    }

    private func assertHARExport(captured: [CaptureEvent], body: [UInt8], timings: HARExchangeTiming) async throws {
        let requestHead = try XCTUnwrap(release022Requests(captured).first)
        let responseHead = try XCTUnwrap(captured.compactMap { event -> CapturedResponseHead? in
            if case let .responseHead(head) = event {
                head
            } else {
                nil
            }
        }
        .first)
        let exchange = HARExchange(
            representation: .original,
            request: requestHead,
            response: responseHead,
            responseBody: HARBody(bytes: body, mimeType: "text/plain", isText: true, truncated: false),
            completed: true,
            timings: timings,
            responseContentBytes: stage2ResponseCount(captured)
        )
        let withoutTiming = HARExchange(
            representation: .original,
            request: requestHead,
            response: responseHead,
            completed: true,
            responseContentBytes: stage2ResponseCount(captured)
        )
        try await assertMissingHARTiming(withoutTiming)
        try await assertRedactedHAR(exchange)
    }

    private func assertMissingHARTiming(_ withoutTiming: HARExchange) async throws {
        let missing = AsyncStream<HARExchange>(bufferingPolicy: .bufferingOldest(1)) { continuation in
            continuation.yield(withoutTiming)
            continuation.finish()
        }
        do {
            try await HARExporter.export(
                missing,
                configuration: HARExportConfiguration(representation: .original),
                writer: Stage2HARBytes()
            )
            XCTFail("HAR exporter invented mandatory timings")
        } catch HARExportError.missingTimings {
            XCTAssertNil(withoutTiming.timings)
        } catch { throw error }
    }

    private func assertRedactedHAR(_ exchange: HARExchange) async throws {
        let entries = AsyncStream<HARExchange>(bufferingPolicy: .bufferingOldest(1)) { continuation in
            continuation.yield(exchange)
            continuation.finish()
        }
        let writer = Stage2HARBytes()
        try await HARExporter.export(
            entries,
            configuration: HARExportConfiguration(representation: .original),
            writer: writer
        )
        let data = Data(writer.bytes)
        let document = try JSONDecoder().decode(Stage2HARDocument.self, from: data)
        XCTAssertEqual(document.log.version, "1.2")
        XCTAssertEqual(document.log.entries.count, 1)
        XCTAssertEqual(document.log.entries.first?.request.method, "GET")
        XCTAssertEqual(document.log.entries.first?.response.status, 200)
        XCTAssertTrue(document.log.entries.first?.request.url.contains("keep=yes") ?? false)
        let output = try XCTUnwrap(String(bytes: writer.bytes, encoding: .utf8))
        for secret in [
            "synthetic-query-secret",
            "synthetic-header-secret",
            "synthetic-cookie-secret",
            "synthetic-response-secret",
            "synthetic-body-secret"
        ] {
            XCTAssertFalse(output.contains(secret), "Export leaked \(secret)")
        }
    }
}

func stage2Milliseconds(_ duration: Duration) -> Double {
    let parts = duration.components
    return Double(parts.seconds) * 1000 + Double(parts.attoseconds) / 1_000_000_000_000_000
}

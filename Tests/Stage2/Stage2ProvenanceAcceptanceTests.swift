import Foundation
import NIOCore
import XCTest

import SwiftMITM

final class Stage2ProvenanceAcceptanceTests: XCTestCase {
    func testSyntheticResponseKeepsEditedRequestAndResponseHeadersInTheirOwnRepresentations() async throws {
        let runtime = HTTPControlRuntime(configuration: try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "request-edit", match: HTTPRuleMatch(phase: .request, path: "/synthetic"),
                action: .editHeaders(remove: ["X-Probe"], append: [HTTPHeaderField(name: "X-Probe", value: "edited")])
            ),
            HTTPRule(
                id: "local-response", match: HTTPRuleMatch(phase: .request, path: "/synthetic"),
                action: .respond(status: 202, headers: [HTTPHeaderField(name: "X-Response", value: "generated")],
                                 body: Data("synthetic-body".utf8))
            )
        ], limits: HTTPControlLimits()))
        let controls = Stage2ControlEvents()
        try await stage2WithHTTPOrigin(runtime: runtime, controlSink: controls) { peer in
            let received = Release022WireObservation()
            let client = try release022WireClient(group: peer.group, port: peer.proxyPort, observation: received)
            try phase4BoundedWait(client.writeAndFlush(ByteBuffer(string:
                "GET http://127.0.0.1:\(peer.origin.port)/synthetic HTTP/1.1\r\n"
                    + "Host: localhost\r\nX-Probe: original\r\n\r\n")))
            let bytes = try received.wait { $0.suffix(14).elementsEqual(Array("synthetic-body".utf8)) }
            let wire = String(decoding: bytes, as: UTF8.self)
            XCTAssertTrue(wire.hasPrefix("HTTP/1.1 202 "))
            XCTAssertTrue(wire.lowercased().contains("x-response: generated\r\n"))
            XCTAssertFalse(wire.lowercased().contains("x-probe:"))
            XCTAssertEqual(peer.origin.acceptedConnections, 0)
            XCTAssertTrue(peer.origin.requests.isEmpty)
            let original = try XCTUnwrap(release022Requests(peer.raw.events).first)
            XCTAssertEqual(original.headers.first { $0.name.lowercased() == "x-probe" }?.value, "original")
            XCTAssertNil(original.headers.first { $0.name.lowercased() == "x-response" })
            try await stage2Await {
                controls.events.contains {
                    if case .effectiveHead(let id, .request, _, _) = $0 {
                        id == original.id
                    } else {
                        false
                    }
                }
            }
            let requests = controls.events.compactMap { event -> [HTTPHeaderField]? in
                if case .effectiveHead(let id, .request, let headers, _) = event, id == original.id {
                    headers
                } else {
                    nil
                }
            }
            XCTAssertEqual(requests.count, 1)
            let effective = try XCTUnwrap(requests.first)
            XCTAssertEqual(effective.first { $0.name.lowercased() == "x-probe" }?.value, "edited")
            XCTAssertNil(effective.first { $0.name.lowercased() == "x-response" })
            try await client.close().get()
        }
    }
}

import XCTest

@testable import SwiftMITM

final class HTTP1UpgradeCorrelationParserTests: XCTestCase {
    func testUpgradeRequestRemainsReadyForHTTPUntilCorrelatedResponse() {
        let parser = HTTP1MessageParser(mode: .request)
        var outputs: [HTTP1ParserOutput] = []
        for input in [
            "GET /socket HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
            "GET /after HTTP/1.1\r\nHost: example.com\r\n\r\n"
        ] {
            parser.feed(Array(input.utf8), requestProvider: { nil }, emit: { outputs.append($0) })
        }
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "GET",
                    path: "/socket",
                    headers: [
                        HTTPHeaderField(name: "Upgrade", value: "websocket"),
                        HTTPHeaderField(name: "Connection", value: "Upgrade")
                    ]
                ),
                .upgradeRequested,
                .requestHead(
                    method: "GET",
                    path: "/after",
                    headers: [HTTPHeaderField(name: "Host", value: "example.com")]
                ),
                .messageComplete
            ]
        )
    }

    func testUncorrelatedSwitchingProtocolsDoesNotEnterTunnel() {
        let parser = HTTP1MessageParser(mode: .response)
        var outputs: [HTTP1ParserOutput] = []
        var requests = [
            HTTP1RequestMetadata(method: "GET", webSocketUpgradeRequested: false),
            HTTP1RequestMetadata(method: "GET", webSocketUpgradeRequested: false)
        ]
        for input in [
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
            "HTTP/1.1 204 No Content\r\n\r\n"
        ] {
            parser.feed(
                Array(input.utf8),
                requestProvider: { requests.first },
                consumeMethod: { requests.removeFirst() },
                emit: { outputs.append($0) }
            )
        }
        XCTAssertEqual(
            outputs,
            [
                .responseHead(
                    status: 101,
                    headers: [
                        HTTPHeaderField(name: "Upgrade", value: "websocket"),
                        HTTPHeaderField(name: "Connection", value: "Upgrade")
                    ]
                ),
                .messageComplete,
                .responseHead(status: 204, headers: []),
                .messageComplete
            ]
        )
    }
}

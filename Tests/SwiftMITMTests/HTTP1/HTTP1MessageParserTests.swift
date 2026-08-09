import XCTest

@testable import SwiftMITM

final class HTTP1MessageParserTests: XCTestCase {
    private func parse(
        mode: HTTP1ParserMode,
        feeds: [String],
        methods: [String] = [],
        finish: Bool = false
    ) -> [HTTP1ParserOutput] {
        let parser = HTTP1MessageParser(mode: mode)
        var outputs: [HTTP1ParserOutput] = []
        var methodQueue = methods
        for feed in feeds {
            parser.feed(
                Array(feed.utf8),
                methodProvider: { methodQueue.first },
                consumeMethod: {
                    if !methodQueue.isEmpty {
                        methodQueue.removeFirst()
                    }
                },
                emit: { outputs.append($0) }
            )
        }
        if finish {
            parser.finish { outputs.append($0) }
        }
        return outputs
    }

    func testRequestWithContentLength() {
        let outputs = parse(mode: .request, feeds: ["POST /x HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"])
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/x",
                    headers: [HTTPHeaderField(name: "Content-Length", value: "5")]
                ),
                .bodyChunk(byteCount: 5),
                .messageComplete
            ]
        )
    }

    func testRequestWithoutBody() {
        let outputs = parse(mode: .request, feeds: ["GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"])
        XCTAssertEqual(
            outputs,
            [
                .requestHead(method: "GET", path: "/", headers: [HTTPHeaderField(name: "Host", value: "example.com")]),
                .messageComplete
            ]
        )
    }

    func testRequestChunkedCountsPayloadOnly() {
        let body = "3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"
        let outputs = parse(mode: .request, feeds: ["POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n" + body])
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/c",
                    headers: [HTTPHeaderField(name: "Transfer-Encoding", value: "chunked")]
                ),
                .bodyChunk(byteCount: 3),
                .bodyChunk(byteCount: 2),
                .messageComplete
            ]
        )
    }

    func testChunkedTrailersEmitBeforeMessageComplete() {
        let outputs = parse(
            mode: .request,
            feeds: [
                "POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nDigest: sha-256=value\r\nX-End: yes\r\n\r\n"
            ]
        )
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/c",
                    headers: [HTTPHeaderField(name: "Transfer-Encoding", value: "chunked")]
                ),
                .trailers([
                    HTTPHeaderField(name: "Digest", value: "sha-256=value"),
                    HTTPHeaderField(name: "X-End", value: "yes")
                ]),
                .messageComplete
            ]
        )
    }

    func testResponseWithContentLength() {
        let outputs = parse(
            mode: .response,
            feeds: ["HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody"],
            methods: ["GET"]
        )
        XCTAssertEqual(
            outputs,
            [
                .responseHead(status: 200, headers: [HTTPHeaderField(name: "Content-Length", value: "4")]),
                .bodyChunk(byteCount: 4),
                .messageComplete
            ]
        )
    }

    func testResponse204HasNoBody() {
        let outputs = parse(mode: .response, feeds: ["HTTP/1.1 204 No Content\r\n\r\n"], methods: ["GET"])
        XCTAssertEqual(outputs, [.responseHead(status: 204, headers: []), .messageComplete])
    }

    func testInformationalResponsesDoNotCompleteOrConsumeFinalResponse() {
        let outputs = parse(
            mode: .response,
            feeds: [
                "HTTP/1.1 100 Continue\r\n\r\n",
                "HTTP/1.1 103 Early Hints\r\nLink: </style.css>\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody"
            ],
            methods: ["GET"]
        )
        XCTAssertEqual(
            outputs,
            [
                .responseHead(status: 100, headers: []),
                .responseHead(
                    status: 103,
                    headers: [HTTPHeaderField(name: "Link", value: "</style.css>")]
                ),
                .responseHead(status: 200, headers: [HTTPHeaderField(name: "Content-Length", value: "4")]),
                .bodyChunk(byteCount: 4),
                .messageComplete
            ]
        )
    }

    func testSwitchingProtocolsIsTerminalUpgrade() {
        let outputs = parse(
            mode: .response,
            feeds: [
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
                "not another response"
            ],
            methods: ["GET"]
        )
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
                .upgraded
            ]
        )
    }

    func testHeadResponseHasNoBodyDespiteContentLength() {
        let outputs = parse(
            mode: .response,
            feeds: ["HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n"],
            methods: ["HEAD"]
        )
        XCTAssertEqual(
            outputs,
            [
                .responseHead(status: 200, headers: [HTTPHeaderField(name: "Content-Length", value: "10")]),
                .messageComplete
            ]
        )
    }

    func testResponseUntilCloseCompletesOnFinish() {
        let outputs = parse(
            mode: .response,
            feeds: ["HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n", "payload"],
            methods: ["GET"],
            finish: true
        )
        XCTAssertEqual(
            outputs,
            [
                .responseHead(status: 200, headers: [HTTPHeaderField(name: "Content-Type", value: "text/plain")]),
                .bodyChunk(byteCount: 7),
                .messageComplete
            ]
        )
    }

    func testPipelinedKeepAliveRequestsInOneFeed() {
        let pipelined =
            "GET /a HTTP/1.1\r\nHost: x\r\n\r\n"
            + "POST /b HTTP/1.1\r\nContent-Length: 2\r\n\r\nhi"
        let outputs = parse(mode: .request, feeds: [pipelined])
        XCTAssertEqual(
            outputs,
            [
                .requestHead(method: "GET", path: "/a", headers: [HTTPHeaderField(name: "Host", value: "x")]),
                .messageComplete,
                .requestHead(
                    method: "POST",
                    path: "/b",
                    headers: [HTTPHeaderField(name: "Content-Length", value: "2")]
                ),
                .bodyChunk(byteCount: 2),
                .messageComplete
            ]
        )
    }

    func testHeadAndBodySplitAcrossFeeds() {
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nContent-Len", "gth: 4\r\n\r\nab", "cd"]
        )
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/x",
                    headers: [HTTPHeaderField(name: "Content-Length", value: "4")]
                ),
                .bodyChunk(byteCount: 2),
                .bodyChunk(byteCount: 2),
                .messageComplete
            ]
        )
    }

    func testChunkSizeLineAtLimitParses() {
        let sizeLine = String(repeating: "0", count: 8189) + "1\r\n"
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n" + sizeLine + "a\r\n0\r\n\r\n"]
        )
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/x",
                    headers: [HTTPHeaderField(name: "Transfer-Encoding", value: "chunked")]
                ),
                .bodyChunk(byteCount: 1),
                .messageComplete
            ]
        )
    }

    func testChunkSizeLineOverLimitFails() {
        let sizeLine = String(repeating: "0", count: 8190) + "1\r\n"
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n" + sizeLine]
        )
        XCTAssertEqual(outputs.last, .failed)
    }

    func testChunkSizeIntegerOverflowFails() {
        let outputs = parse(
            mode: .request,
            feeds: [
                "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nFFFFFFFFFFFFFFFFF\r\n"
            ]
        )
        XCTAssertEqual(outputs.last, .failed)
    }

    func testChunkDataRequiresExactCRLF() {
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r", "X"]
        )
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/x",
                    headers: [HTTPHeaderField(name: "Transfer-Encoding", value: "chunked")]
                ),
                .bodyChunk(byteCount: 3),
                .failed
            ]
        )
    }

    func testChunkDataCRLFSplitAcrossFeedsParses() {
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r", "\n0\r\n\r\n"]
        )
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/x",
                    headers: [HTTPHeaderField(name: "Transfer-Encoding", value: "chunked")]
                ),
                .bodyChunk(byteCount: 3),
                .messageComplete
            ]
        )
    }
}

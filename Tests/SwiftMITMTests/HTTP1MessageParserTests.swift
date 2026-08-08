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
                methodProvider: { methodQueue.isEmpty ? nil : methodQueue.removeFirst() },
                emit: { outputs.append($0) }
            )
        }
        if finish { parser.finish { outputs.append($0) } }
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
}

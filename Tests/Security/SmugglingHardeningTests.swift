import XCTest

@testable import SwiftMITM

final class HTTP1ParserSmugglingTests: XCTestCase {
    private func parse(mode: HTTP1ParserMode, feeds: [String], methods: [String] = []) -> [HTTP1ParserOutput] {
        let parser = HTTP1MessageParser(mode: mode)
        var outputs: [HTTP1ParserOutput] = []
        var methodQueue = methods
        for feed in feeds {
            parser.feed(
                Array(feed.utf8),
                requestProvider: {
                    methodQueue.isEmpty
                        ? nil
                        : HTTP1RequestMetadata(
                            method: methodQueue.removeFirst(),
                            webSocketUpgradeRequested: false
                        )
                },
                emit: { outputs.append($0) }
            )
        }
        return outputs
    }

    func testNegativeContentLengthFails() {
        let outputs = parse(mode: .request, feeds: ["POST /x HTTP/1.1\r\nContent-Length: -5\r\n\r\nhello"])
        XCTAssertEqual(outputs, [.failed])
    }

    func testConflictingContentLengthFails() {
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nContent-Length: 10\r\nContent-Length: 20\r\n\r\n"]
        )
        XCTAssertEqual(outputs, [.failed])
    }

    func testNonNumericContentLengthFails() {
        let outputs = parse(mode: .request, feeds: ["POST /x HTTP/1.1\r\nContent-Length: 5abc\r\n\r\n"])
        XCTAssertEqual(outputs, [.failed])
    }

    func testDuplicateMatchingContentLengthParses() {
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello"]
        )
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/x",
                    headers: [
                        HTTPHeaderField(name: "Content-Length", value: "5"),
                        HTTPHeaderField(name: "Content-Length", value: "5")
                    ]
                ),
                .bodyChunk(byteCount: 5),
                .messageComplete
            ]
        )
    }

    func testContentLengthPlusTransferEncodingPrefersChunked() {
        let body = "3\r\nabc\r\n0\r\n\r\n"
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n" + body]
        )
        XCTAssertEqual(
            outputs,
            [
                .requestHead(
                    method: "POST",
                    path: "/x",
                    headers: [
                        HTTPHeaderField(name: "Content-Length", value: "100"),
                        HTTPHeaderField(name: "Transfer-Encoding", value: "chunked")
                    ]
                ),
                .bodyChunk(byteCount: 3),
                .messageComplete
            ]
        )
    }

    func testNegativeChunkSizeFailsWithoutIndexRegression() {
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n-1\r\nabc\r\n"]
        )
        XCTAssertEqual(outputs.last, .failed)
    }

    func testNonHexChunkSizeFails() {
        let outputs = parse(
            mode: .request,
            feeds: ["POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nabc\r\n"]
        )
        XCTAssertEqual(outputs.last, .failed)
    }
}

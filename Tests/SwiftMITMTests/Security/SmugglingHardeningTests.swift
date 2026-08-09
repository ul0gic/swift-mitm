import NIOHPACK
import XCTest

@testable import SwiftMITM

final class PseudoHeaderSmugglingTests: XCTestCase {
    private func makeHeaders(_ pairs: [(String, String)]) -> HPACKHeaders {
        var headers = HPACKHeaders()
        for (name, value) in pairs {
            headers.add(name: name, value: value)
        }
        return headers
    }

    private func base(path: String = "/", method: String = "GET") -> [(String, String)] {
        [(":method", method), (":scheme", "https"), (":authority", "example.com"), (":path", path)]
    }

    func testSpaceInMethodRejected() {
        let headers = makeHeaders([
            (":method", "GET /evil HTTP/1.1"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/")
        ])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.invalidMethod))
    }

    func testNonTokenMethodRejected() {
        var pairs = base()
        pairs[0] = (":method", "GE(T")
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(makeHeaders(pairs)), .failure(.invalidMethod))
    }

    func testInvalidSchemeRejected() {
        var pairs = base()
        pairs[1] = (":scheme", "ht tp")
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(makeHeaders(pairs)), .failure(.invalidScheme))
    }

    func testSpaceInPathRejected() {
        let headers = makeHeaders(base(path: "/api v1 HTTP/1.1"))
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.invalidPath))
    }

    func testTabInPathRejected() {
        let headers = makeHeaders(base(path: "/api\tv1"))
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.illegalCharacterInPseudoHeader(":path"))
        )
    }

    func testSpaceInAuthorityRejected() {
        var pairs = base()
        pairs[2] = (":authority", "example.com evil.com")
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(makeHeaders(pairs)), .failure(.invalidAuthority))
    }

    func testSpaceInHeaderNameRejected() {
        let headers = makeHeaders(base() + [("x evil", "1")])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.invalidHeaderName("x evil")))
    }

    func testColonInHeaderNameRejected() {
        let headers = makeHeaders(base() + [("x:smuggled", "1")])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.invalidHeaderName("x:smuggled")))
    }

    func testEmptyHeaderNameRejected() {
        let headers = makeHeaders(base() + [("", "1")])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.emptyHeaderName))
    }

    func testNonNumericContentLengthRejected() {
        let headers = makeHeaders(base(method: "POST") + [("content-length", "5x")])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.illegalContentLength))
    }

    func testUnicodeDigitContentLengthRejected() {
        let headers = makeHeaders(base(method: "POST") + [("content-length", "\u{00B2}")])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.illegalContentLength))
    }

    func testNegativeContentLengthRejected() {
        let headers = makeHeaders(base(method: "POST") + [("content-length", "-5")])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.illegalContentLength))
    }

    func testEmptyContentLengthRejected() {
        let headers = makeHeaders(base(method: "POST") + [("content-length", "")])
        XCTAssertEqual(PseudoHeaderSanitizer.sanitizeRequest(headers), .failure(.illegalContentLength))
    }

    func testDuplicateMatchingContentLengthAccepted() {
        let headers = makeHeaders(base(method: "POST") + [("content-length", "10"), ("content-length", "10")])
        guard case .success = PseudoHeaderSanitizer.sanitizeRequest(headers) else {
            return XCTFail("matching duplicate content-length should pass")
        }
    }
}

final class HTTP1ParserSmugglingTests: XCTestCase {
    private func parse(mode: HTTP1ParserMode, feeds: [String], methods: [String] = []) -> [HTTP1ParserOutput] {
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

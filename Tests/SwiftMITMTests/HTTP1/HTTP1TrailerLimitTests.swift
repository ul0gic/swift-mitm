import XCTest

@testable import SwiftMITM

final class HTTP1TrailerLimitTests: XCTestCase {
    func testAggregateTrailersAtLimitParse() {
        let trailers = "X:" + String(repeating: "a", count: 262_138) + "\r\n\r\n"
        XCTAssertEqual(parse(trailers: trailers).last, .messageComplete)
    }

    func testAggregateTrailersOverLimitFail() {
        let trailers = "X:" + String(repeating: "a", count: 262_139) + "\r\n\r\n"
        XCTAssertEqual(parse(trailers: trailers).last, .failed)
    }

    private func parse(trailers: String) -> [HTTP1ParserOutput] {
        let parser = HTTP1MessageParser(mode: .request)
        var outputs: [HTTP1ParserOutput] = []
        parser.feed(
            Array(("POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n" + trailers).utf8),
            methodProvider: { nil },
            emit: { outputs.append($0) }
        )
        return outputs
    }
}

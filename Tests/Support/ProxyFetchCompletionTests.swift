import NIOEmbedded
import XCTest

final class ProxyFetchCompletionTests: XCTestCase {
    func testTimeoutReportsPhaseAndReceivedByteProgress() {
        let completion = ProxyFetchCompletion(eventLoop: EmbeddedEventLoop())
        completion.advance(to: .request)
        completion.addReceivedBytes(98_303)
        defer { completion.complete(.success(98_303)) }

        guard case let .fetchTimeout(phase, bytesReceived) = completion.timeoutError() else {
            return XCTFail("expected a fetch timeout")
        }
        XCTAssertEqual(phase, "response")
        XCTAssertEqual(bytesReceived, 98_303)
    }

    func testCompletionIsExactlyOnce() throws {
        let completion = ProxyFetchCompletion(eventLoop: EmbeddedEventLoop())
        completion.complete(.success(42))
        completion.complete(.failure(ProxyTestError.connectionClosedBeforeResponseEnd))

        XCTAssertEqual(try completion.futureResult.wait(), 42)
    }
}

import NIOHPACK
import XCTest

@testable import SwiftMITM

final class HTTP2ExtendedConnectStreamCoordinatorTests: XCTestCase {
    func testOrdinaryRequestsAndConnectWithoutProtocolRemainHTTP() {
        for headers in [ordinaryRequestHeaders(), ordinaryConnectHeaders()] {
            var coordinator = HTTP2ExtendedConnectStreamCoordinator()
            XCTAssertEqual(
                coordinator.receiveInitialRequest(
                    headers: headers,
                    endStream: false,
                    extendedConnectEnabled: false
                ),
                .forwardHTTP
            )
            XCTAssertEqual(coordinator.state, .ordinaryHTTP)
            XCTAssertEqual(coordinator.receiveData(direction: .request, endStream: true), .forwardHTTP)
            XCTAssertEqual(coordinator.state, .ordinaryHTTP)
            XCTAssertEqual(coordinator.receiveResponse(status: 200, endStream: true), .forwardHTTP)
            XCTAssertEqual(coordinator.state, .ended)
        }
    }

    func testValidExtendedConnectWaitsForFinalSuccessThenOpensWebSocket() {
        var coordinator = HTTP2ExtendedConnectStreamCoordinator()
        XCTAssertEqual(
            coordinator.receiveInitialRequest(
                headers: extendedConnectHeaders(),
                endStream: false,
                extendedConnectEnabled: true
            ),
            .awaitFinalResponse
        )
        XCTAssertEqual(coordinator.state, .candidateExtendedConnect)
        XCTAssertEqual(coordinator.receiveResponse(status: 103, endStream: false), .awaitFinalResponse)
        XCTAssertEqual(coordinator.state, .candidateExtendedConnect)
        XCTAssertEqual(coordinator.receiveResponse(status: 200, endStream: false), .openWebSocket)
        XCTAssertEqual(coordinator.state, .acceptedWebSocket)
        XCTAssertEqual(coordinator.receiveData(direction: .request, endStream: true), .forwardWebSocket)
        XCTAssertEqual(coordinator.state, .acceptedWebSocket)
        XCTAssertEqual(coordinator.receiveData(direction: .response, endStream: true), .forwardWebSocket)
        XCTAssertEqual(coordinator.state, .ended)
    }

    func testExtendedConnectRequiresCapabilityAndCompletePseudoHeaders() {
        var disabled = HTTP2ExtendedConnectStreamCoordinator()
        XCTAssertEqual(
            disabled.receiveInitialRequest(
                headers: extendedConnectHeaders(),
                endStream: false,
                extendedConnectEnabled: false
            ),
            .streamError
        )
        XCTAssertEqual(disabled.state, .failed)

        var malformedHeaders = extendedConnectHeaders()
        malformedHeaders.remove(name: ":authority")
        var malformed = HTTP2ExtendedConnectStreamCoordinator()
        XCTAssertEqual(
            malformed.receiveInitialRequest(
                headers: malformedHeaders,
                endStream: false,
                extendedConnectEnabled: true
            ),
            .streamError
        )
        XCTAssertEqual(malformed.state, .failed)
    }

    func testExtendedConnectEndingWithRequestHeadersIsRejected() {
        var coordinator = HTTP2ExtendedConnectStreamCoordinator()
        XCTAssertEqual(
            coordinator.receiveInitialRequest(
                headers: extendedConnectHeaders(),
                endStream: true,
                extendedConnectEnabled: true
            ),
            .streamError
        )
        XCTAssertEqual(coordinator.state, .failed)
    }

    func testFinalNonSuccessRemainsHTTP() {
        var coordinator = candidate()
        XCTAssertEqual(coordinator.receiveResponse(status: 403, endStream: false), .forwardHTTP)
        XCTAssertEqual(coordinator.state, .rejectedHTTP)
        XCTAssertEqual(coordinator.receiveData(direction: .request, endStream: true), .forwardHTTP)
        XCTAssertEqual(coordinator.state, .rejectedHTTP)
        XCTAssertEqual(coordinator.receiveData(direction: .response, endStream: true), .forwardHTTP)
        XCTAssertEqual(coordinator.state, .ended)
    }

    func testSuccessfulResponseWithEndStreamDoesNotOpenWebSocket() {
        var coordinator = candidate()
        XCTAssertEqual(coordinator.receiveResponse(status: 204, endStream: true), .forwardHTTP)
        XCTAssertEqual(coordinator.state, .rejectedHTTP)
        XCTAssertEqual(coordinator.receiveData(direction: .request, endStream: true), .forwardHTTP)
        XCTAssertEqual(coordinator.state, .ended)
    }

    func testDataBeforeSuccessfulFinalResponseFailsWithoutBufferingState() {
        var coordinator = candidate()
        XCTAssertEqual(coordinator.receiveData(direction: .request, endStream: false), .streamError)
        XCTAssertEqual(coordinator.state, .failed)
    }

    func testInformationalResponseCannotEndCandidateStream() {
        var coordinator = candidate()
        XCTAssertEqual(coordinator.receiveResponse(status: 103, endStream: true), .streamError)
        XCTAssertEqual(coordinator.state, .failed)
    }

    func testInvalidProtocolValueAndDuplicatePseudoHeaderAreStreamErrors() {
        var invalidProtocolHeaders = extendedConnectHeaders()
        invalidProtocolHeaders.replaceOrAdd(name: ":protocol", value: "not-websocket")
        var invalidProtocol = HTTP2ExtendedConnectStreamCoordinator()
        XCTAssertEqual(
            invalidProtocol.receiveInitialRequest(
                headers: invalidProtocolHeaders,
                endStream: false,
                extendedConnectEnabled: true
            ),
            .streamError
        )

        var duplicateHeaders = extendedConnectHeaders()
        duplicateHeaders.add(name: ":path", value: "/second")
        var duplicate = HTTP2ExtendedConnectStreamCoordinator()
        XCTAssertEqual(
            duplicate.receiveInitialRequest(
                headers: duplicateHeaders,
                endStream: false,
                extendedConnectEnabled: true
            ),
            .streamError
        )
    }

    func testExtendedConnectRejectsHTTP1ConnectionHeaders() {
        for forbiddenName in ["connection", "upgrade"] {
            var headers = extendedConnectHeaders()
            headers.add(name: forbiddenName, value: "websocket")
            var coordinator = HTTP2ExtendedConnectStreamCoordinator()
            XCTAssertEqual(
                coordinator.receiveInitialRequest(
                    headers: headers,
                    endStream: false,
                    extendedConnectEnabled: true
                ),
                .streamError
            )
            XCTAssertEqual(coordinator.state, .failed)
        }
    }

    private func candidate() -> HTTP2ExtendedConnectStreamCoordinator {
        var coordinator = HTTP2ExtendedConnectStreamCoordinator()
        XCTAssertEqual(
            coordinator.receiveInitialRequest(
                headers: extendedConnectHeaders(),
                endStream: false,
                extendedConnectEnabled: true
            ),
            .awaitFinalResponse
        )
        return coordinator
    }

    private func extendedConnectHeaders() -> HPACKHeaders {
        HPACKHeaders([
            (":method", "CONNECT"),
            (":protocol", "websocket"),
            (":scheme", "https"),
            (":path", "/socket"),
            (":authority", "example.com")
        ])
    }

    private func ordinaryRequestHeaders() -> HPACKHeaders {
        HPACKHeaders([
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            (":authority", "example.com")
        ])
    }

    private func ordinaryConnectHeaders() -> HPACKHeaders {
        HPACKHeaders([
            (":method", "CONNECT"),
            (":authority", "example.com")
        ])
    }
}

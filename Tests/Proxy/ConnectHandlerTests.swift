import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOHTTP1
import XCTest

@testable import SwiftMITM

final class ConnectHandlerTests: XCTestCase {
    func testValidConnectRepliesAndReportsAuthority() throws {
        let captured = NIOLockedValueBox<String?>(nil)
        let channel = EmbeddedChannel()
        try channel.pipeline.syncOperations.addHandler(
            ConnectHandler { _, authority in captured.withLockedValue { $0 = authority } }
        )

        var head = HTTPRequestHead(version: .http1_1, method: .CONNECT, uri: "example.com:443")
        head.headers.add(name: "host", value: "example.com:443")
        try channel.writeInbound(HTTPServerRequestPart.head(head))
        try channel.writeInbound(HTTPServerRequestPart.end(nil))

        guard case .head(let response)? = try channel.readOutbound(as: HTTPServerResponsePart.self) else {
            return XCTFail("expected a response head")
        }
        XCTAssertEqual(response.status.code, 200)
        XCTAssertEqual(captured.withLockedValue { $0 }, "example.com:443")
    }

    func testEncodedConnectSuccessHasNoHTTPBodyFraming() throws {
        let channel = EmbeddedChannel()
        var encoderConfiguration = HTTPResponseEncoder.Configuration()
        encoderConfiguration.automaticallySetFramingHeaders = false
        try channel.pipeline.syncOperations.addHandlers([
            HTTPResponseEncoder(configuration: encoderConfiguration),
            ByteToMessageHandler(HTTPRequestDecoder(leftOverBytesStrategy: .forwardBytes)),
            ConnectHandler { _, _ in }
        ])

        var request = channel.allocator.buffer(capacity: 64)
        request.writeString("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
        try channel.writeInbound(request)

        guard case .byteBuffer(let response)? = try channel.readOutbound(as: IOData.self) else {
            return XCTFail("expected an encoded CONNECT response")
        }
        XCTAssertEqual(
            response.getString(at: response.readerIndex, length: response.readableBytes),
            "HTTP/1.1 200 Connection Established\r\n\r\n"
        )
        guard case .byteBuffer(let end)? = try channel.readOutbound(as: IOData.self) else {
            return XCTFail("expected an empty response end")
        }
        XCTAssertEqual(end.readableBytes, 0)
        XCTAssertNil(try channel.readOutbound(as: IOData.self))
        XCTAssertNoThrow(try channel.finish())
    }

    func testNonConnectMethodRejected() throws {
        let channel = EmbeddedChannel()
        try channel.pipeline.syncOperations.addHandler(ConnectHandler { _, _ in })

        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: "/")
        try channel.writeInbound(HTTPServerRequestPart.head(head))

        guard case .head(let response)? = try channel.readOutbound(as: HTTPServerResponsePart.self) else {
            return XCTFail("expected a response head")
        }
        XCTAssertEqual(response.status, .methodNotAllowed)
    }

    func testMalformedAuthorityRejected() {
        XCTAssertFalse(ConnectHandler.isValidAuthority("example.com:443\r\nHost: evil"))
        XCTAssertFalse(ConnectHandler.isValidAuthority("example.com"))
        XCTAssertFalse(ConnectHandler.isValidAuthority("example.com:0"))
        XCTAssertFalse(ConnectHandler.isValidAuthority("example.com:99999"))
        XCTAssertFalse(ConnectHandler.isValidAuthority(":443"))
        XCTAssertTrue(ConnectHandler.isValidAuthority("example.com:443"))
        XCTAssertTrue(ConnectHandler.isValidAuthority("127.0.0.1:8443"))
    }
}

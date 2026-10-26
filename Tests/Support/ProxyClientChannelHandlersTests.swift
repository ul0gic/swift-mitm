import NIOEmbedded
import NIOTLS
import XCTest

@testable import SwiftMITM

final class ProxyClientChannelHandlersTests: XCTestCase {
    func testTLSInstallationFailureCompletesAllPromisesAndClosesChannel() throws {
        let channel = EmbeddedChannel()
        let client = ProxyTestClient(group: channel.eventLoop)
        let authority = try CertificateAuthority.generate().authority
        let installation = try client.installTLS(
            on: channel,
            serverHostname: "127.0.0.1",
            mitmCACertificatePEM: authority.caCertificatePEM,
            applicationProtocols: ["h2"],
            configuresHTTP2: true
        )

        channel.embeddedEventLoop.run()

        XCTAssertThrowsError(try installation.installed.wait())
        XCTAssertThrowsError(try installation.handshake.wait())
        XCTAssertThrowsError(try XCTUnwrap(installation.multiplexer).wait())
        XCTAssertFalse(channel.isActive)
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testTLSHandshakeCompletionIgnoresEveryLaterTerminalPath() throws {
        let channel = EmbeddedChannel()
        let completion = ProxyClientPromise<String?>(eventLoop: channel.eventLoop)
        try channel.pipeline.syncOperations.addHandler(TLSHandshakeProbe(completion: completion))

        channel.pipeline.fireUserInboundEventTriggered(TLSUserEvent.handshakeCompleted(negotiatedProtocol: nil))
        channel.pipeline.fireErrorCaught(ProxyTestError.tlsClosedBeforeHandshake)
        channel.embeddedEventLoop.run()

        XCTAssertNil(try completion.futureResult.wait())
        XCTAssertFalse(channel.isActive)
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testIPLiteralUsesNilSNIButDNSNameRemainsAvailableForVerification() {
        XCTAssertNil(ProxyTestClient.tlsServerHostname(for: "127.0.0.1"))
        XCTAssertNil(ProxyTestClient.tlsServerHostname(for: "::1"))
        XCTAssertEqual(ProxyTestClient.tlsServerHostname(for: "localhost"), "localhost")
    }
}

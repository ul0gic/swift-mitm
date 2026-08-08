import NIOCore
import NIOPosix
import NIOSSL
import NIOTLS
import XCTest

@testable import SwiftMITM

final class TLSInterceptionTests: XCTestCase {
    func testPerSNILeafMintingAndALPNNegotiation() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let ca = try CertificateAuthority()
        let serverContext = try TLSTermination(authority: ca).makeServerContext()

        let serverALPN = group.next().makePromise(of: String?.self)
        let server = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(NIOSSLServerHandler(context: serverContext))
                    try channel.pipeline.syncOperations.addHandler(HandshakeProbe(promise: serverALPN))
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
        defer { try? server.close().wait() }
        let port = try XCTUnwrap(server.localAddress?.port)

        var clientConfig = TLSConfiguration.makeClientConfiguration()
        clientConfig.trustRoots = .certificates([
            try NIOSSLCertificate(bytes: Array(ca.caCertificatePEM.utf8), format: .pem)
        ])
        clientConfig.certificateVerification = .fullVerification
        clientConfig.applicationProtocols = ["h2", "http/1.1"]
        let clientContext = try NIOSSLContext(configuration: clientConfig)

        let clientALPN = group.next().makePromise(of: String?.self)
        let client = try ClientBootstrap(group: group)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        NIOSSLClientHandler(context: clientContext, serverHostname: "example.com")
                    )
                    try channel.pipeline.syncOperations.addHandler(HandshakeProbe(promise: clientALPN))
                }
            }
            .connect(host: "127.0.0.1", port: port)
            .wait()
        defer { try? client.close().wait() }

        XCTAssertEqual(try clientALPN.futureResult.wait(), "h2")
        XCTAssertEqual(try serverALPN.futureResult.wait(), "h2")
    }
}

private final class HandshakeProbe: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let promise: EventLoopPromise<String?>

    init(promise: EventLoopPromise<String?>) {
        self.promise = promise
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let negotiatedProtocol) = event {
            promise.succeed(negotiatedProtocol)
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        promise.fail(error)
        context.close(promise: nil)
    }
}

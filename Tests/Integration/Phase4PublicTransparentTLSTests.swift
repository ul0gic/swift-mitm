import NIOCore
import NIOHPACK
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import NIOTLS
import XCTest

import SwiftMITM

final class Phase4PublicTransparentTLSTests: XCTestCase {
    func testTransparentTLSRoutesOriginalDestinationDespiteDifferingSNIAndCapturesHTTP11() async throws {
        try await assertTLSHTTPExchange(alpn: "http/1.1", serverHostname: "localhost", expectedSNI: "localhost")
    }

    func testTransparentTLSWithoutSNIUsesIPIdentityAndCapturesHTTP2() async throws {
        try await assertTLSHTTPExchange(alpn: "h2", serverHostname: nil, expectedSNI: nil)
    }

    func testECHAndUnsupportedALPNFallbackToExactOpaqueForwarding() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(group: group, sink: sink, opaqueCaptureByteLimit: 4_096)

        do {
            let proxyPort = try await proxy.start(port: 0)
            let ech = Phase4DirectTLSClient(group: group).rawClientHello(for: .encryptedClientHello)
            let unsupported = unsupportedALPNVector()
            try assertOpaqueTLSVector(
                ech,
                expectedSNI: nil,
                expectedFlowCount: 1,
                proxyPort: proxyPort,
                group: group,
                sink: sink
            )
            try assertOpaqueTLSVector(
                unsupported,
                expectedSNI: nil,
                expectedFlowCount: 2,
                proxyPort: proxyPort,
                group: group,
                sink: sink
            )
            try await proxy.stop()
            try await group.shutdownGracefully()
        } catch {
            try? await proxy.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertTLSHTTPExchange(
        alpn: String,
        serverHostname: String?,
        expectedSNI: String?
    ) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 32, applicationProtocols: [alpn])
        let sink = Phase4CaptureSink()
        let mitmCA = try CertificateAuthority.generate().authority
        let proxy = try makeProxy(group: group, sink: sink, authority: mitmCA)
        let client = Phase4TransparentTLSHTTPClient(group: group)

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let bodyBytes = try client.fetch(
                proxyPort: proxyPort,
                originPort: origin.localPort,
                serverHostname: serverHostname,
                alpn: alpn,
                mitmRootPEM: mitmCA.caCertificatePEM
            )
            let snapshot = try sink.wait {
                $0.requestHeads.count == 1 && $0.responseHeads.count == 1 && $0.eventKinds.contains(.responseEnd)
            }

            XCTAssertEqual(bodyBytes, 32)
            let request = try XCTUnwrap(snapshot.requestHeads.first)
            XCTAssertEqual(request.scheme, "https")
            XCTAssertEqual(request.version, alpn == "h2" ? .http2 : .http11)
            XCTAssertEqual(snapshot.responseHeads.first?.version, request.version)
            XCTAssertEqual(snapshot.responseBodies.flatMap(\.bytes), Array(repeating: 0x41, count: 32))
            XCTAssertEqual(snapshot.responseBodies.reduce(0) { $0 + $1.byteCount }, 32)
            XCTAssertEqual(snapshot.responseEnds, [.init(requestID: request.id, truncated: false)])
            assertTarget(request.target, originPort: origin.localPort, expectedSNI: expectedSNI)
            client.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            client.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func assertOpaqueTLSVector(
        _ bytes: [UInt8],
        expectedSNI: String?,
        expectedFlowCount: Int,
        proxyPort: Int,
        group: EventLoopGroup,
        sink: Phase4CaptureSink
    ) throws {
        let peer = Phase2LoopbackBytePeer(
            group: group,
            configuration: .init(expectedBytes: bytes, maximumInboundBytes: bytes.count)
        )
        let forwarder = Phase4ProxyV2Forwarder(group: group)
        defer {
            forwarder.stop()
            peer.stop()
        }

        try peer.start()
        _ = try forwarder.connect(listenerPort: proxyPort)
        let header = ingressHeader(destinationPort: peer.localPort)
        try forwarder.send(header: header, applicationBytes: bytes, delivery: .coalesced)
        let observation = try peer.observation.wait()
        XCTAssertEqual(try observation.futureResult.wait(), bytes)
        let snapshot = try sink.wait { $0.opaqueFlows.count >= expectedFlowCount }
        let flow = try XCTUnwrap(snapshot.opaqueFlows.last)
        XCTAssertEqual(flow.target.destination.address, "127.0.0.1")
        XCTAssertEqual(flow.target.destination.port, peer.localPort)
        XCTAssertEqual(flow.target.tlsServerName, expectedSNI)
        XCTAssertEqual(
            snapshot.opaqueData.filter { $0.flowID == flow.id }.flatMap(\.bytes),
            bytes
        )
    }

    private func assertTarget(_ target: CapturedTarget?, originPort: Int, expectedSNI: String?) {
        XCTAssertEqual(target?.destination.address, "127.0.0.1")
        XCTAssertEqual(target?.destination.port, originPort)
        XCTAssertEqual(target?.logicalAuthority, "127.0.0.1:\(originPort)")
        XCTAssertEqual(target?.tlsServerName, expectedSNI)
        XCTAssertEqual(target?.ingressProvenance, .trustedProxyV2)
        XCTAssertEqual(target?.originalClient?.address, "192.0.2.30")
        XCTAssertEqual(target?.originalClient?.port, 50_002)
    }

    private func makeProxy(
        group: EventLoopGroup,
        sink: Phase4CaptureSink,
        authority: CertificateAuthority? = nil,
        opaqueCaptureByteLimit: Int = 0
    ) throws -> ProxyServer {
        let ingress = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        return ProxyServer(
            certificateAuthority: try authority ?? CertificateAuthority.generate().authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(verifyCertificate: false),
            egressPolicy: .init(allowInternal: true),
            ingress: .trustedProxyV2(ingress),
            captureBodyLimit: 64,
            opaqueCaptureByteLimit: opaqueCaptureByteLimit
        )
    }

    private func ingressHeader(destinationPort: Int) -> Phase4ProxyV2Header {
        .ipv4(
            source: [192, 0, 2, 30],
            destination: [127, 0, 0, 1],
            sourcePort: 50_002,
            destinationPort: destinationPort
        )
    }

    private func unsupportedALPNVector() -> [UInt8] {
        var bytes = Phase2TLSIngressVectors.http11ClientHello
        let supported = Array("http/1.1".utf8)
        guard let range = bytes.firstRange(of: supported) else { return [] }
        bytes.replaceSubrange(range, with: Array("opaque/1".utf8))
        return bytes
    }
}

private final class Phase4TransparentTLSHTTPClient {
    private let group: EventLoopGroup
    private var channel: Channel?
    private var responseCompletion: Phase2FixtureCompletion<Int>?

    init(group: EventLoopGroup) {
        self.group = group
    }

    func fetch(
        proxyPort: Int,
        originPort: Int,
        serverHostname: String?,
        alpn: String,
        mitmRootPEM: String
    ) throws -> Int {
        let channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .connect(host: "127.0.0.1", port: proxyPort))
        self.channel = channel
        let header = Phase4ProxyV2Header.ipv4(
            source: [192, 0, 2, 30],
            destination: [127, 0, 0, 1],
            sourcePort: 50_002,
            destinationPort: originPort
        )
        try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: header.bytes)))
        try installTLS(
            channel: channel,
            serverHostname: serverHostname,
            alpn: alpn,
            mitmRootPEM: mitmRootPEM
        )
        let completion = Phase2FixtureCompletion<Int>(eventLoop: channel.eventLoop, timeout: .seconds(5))
        responseCompletion = completion
        if alpn == "h2" {
            try installHTTP2(channel: channel, originPort: originPort, completion: completion)
        } else {
            try installHTTP1(channel: channel, originPort: originPort, completion: completion)
        }
        return try completion.futureResult.wait()
    }

    func stop() {
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
        responseCompletion?.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
    }

    private func installTLS(
        channel: Channel,
        serverHostname: String?,
        alpn: String,
        mitmRootPEM: String
    ) throws {
        var configuration = TLSConfiguration.makeClientConfiguration()
        configuration.applicationProtocols = [alpn]
        configuration.certificateVerification = .fullVerification
        configuration.trustRoots = .certificates([
            try NIOSSLCertificate(bytes: Array(mitmRootPEM.utf8), format: .pem)
        ])
        let context = try NIOSSLContext(configuration: configuration)
        let handshake = Phase2FixtureCompletion<String?>(eventLoop: channel.eventLoop, timeout: .seconds(5))
        try phase4BoundedWait(channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandlers([
                NIOSSLClientHandler(context: context, serverHostname: serverHostname),
                Phase4TransparentTLSHandshakeHandler(completion: handshake)
            ])
        })
        XCTAssertEqual(try handshake.futureResult.wait(), alpn)
    }

    private func installHTTP1(
        channel: Channel,
        originPort: Int,
        completion: Phase2FixtureCompletion<Int>
    ) throws {
        try phase4BoundedWait(channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandlers([
                HTTPRequestEncoder(),
                ByteToMessageHandler(HTTPResponseDecoder()),
                Phase4TransparentH1ClientHandler(
                    authority: "127.0.0.1:\(originPort)",
                    completion: completion
                )
            ])
        })
    }

    private func installHTTP2(
        channel: Channel,
        originPort: Int,
        completion: Phase2FixtureCompletion<Int>
    ) throws {
        let multiplexer = try phase4BoundedWait(channel.configureHTTP2Pipeline(
            mode: .client,
            connectionConfiguration: .init(),
            streamConfiguration: .init()
        ) { $0.close() })
        _ = try phase4BoundedWait(multiplexer.createStreamChannel { stream in
            stream.eventLoop.makeCompletedFuture {
                try stream.pipeline.syncOperations.addHandler(Phase4TransparentH2ClientHandler(
                    authority: "127.0.0.1:\(originPort)",
                    completion: completion
                ))
            }
        })
    }
}

private final class Phase4TransparentTLSHandshakeHandler: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let completion: Phase2FixtureCompletion<String?>

    init(completion: Phase2FixtureCompletion<String?>) {
        self.completion = completion
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let protocolName) = event {
            completion.complete(.success(protocolName))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

private final class Phase4TransparentH1ClientHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundIn = HTTPClientRequestPart
    typealias OutboundOut = HTTPClientRequestPart

    private let authority: String
    private let completion: Phase2FixtureCompletion<Int>
    private var bodyBytes = 0

    init(authority: String, completion: Phase2FixtureCompletion<Int>) {
        self.authority = authority
        self.completion = completion
    }

    func handlerAdded(context: ChannelHandlerContext) {
        var headers = HTTPHeaders()
        headers.add(name: "host", value: authority)
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: "/stream", headers: headers)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .body(let buffer):
            bodyBytes += buffer.readableBytes
        case .end:
            completion.complete(.success(bodyBytes))
        case .head:
            break
        }
    }
}

private final class Phase4TransparentH2ClientHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias OutboundIn = HTTP2Frame.FramePayload
    typealias OutboundOut = HTTP2Frame.FramePayload

    private let authority: String
    private let completion: Phase2FixtureCompletion<Int>
    private var bodyBytes = 0

    init(authority: String, completion: Phase2FixtureCompletion<Int>) {
        self.authority = authority
        self.completion = completion
    }

    func channelActive(context: ChannelHandlerContext) {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: "GET")
        headers.add(name: ":path", value: "/stream")
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":authority", value: authority)
        context.writeAndFlush(wrapOutboundOut(.headers(.init(headers: headers, endStream: true))), promise: nil)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .data(let data):
            bodyBytes += data.data.readableBytes
            if data.endStream {
                completion.complete(.success(bodyBytes))
            }
        case .headers(let headers) where headers.endStream:
            completion.complete(.success(bodyBytes))
        default:
            break
        }
    }
}

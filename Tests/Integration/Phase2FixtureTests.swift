import NIOCore
import NIOPosix
import XCTest

final class Phase2FixtureTests: XCTestCase {
    func testProxyV2CorpusHasExactGoldenBytesAndBoundedMalformedCases() {
        XCTAssertEqual(Array(Phase2ProxyV2Corpus.ipv4.bytes.prefix(12)), Phase2ProxyV2Corpus.signature)
        XCTAssertEqual(Phase2ProxyV2Corpus.ipv4.bytes.count, 33)
        XCTAssertEqual(Array(Phase2ProxyV2Corpus.ipv4.bytes[14 ... 15]), [0x00, 0x11])
        XCTAssertEqual(Phase2ProxyV2Corpus.ipv6.bytes.count, 52)
        XCTAssertEqual(Array(Phase2ProxyV2Corpus.ipv6.bytes[14 ... 15]), [0x00, 0x24])
        XCTAssertEqual(Set(Phase2ProxyV2Corpus.malformed.map(\.name)).count, Phase2ProxyV2Corpus.malformed.count)
        XCTAssertTrue(Phase2ProxyV2Corpus.malformed.allSatisfy { $0.bytes.count <= Int(UInt16.max) + 16 })
        XCTAssertEqual(Phase2ProxyV2Corpus.replay, [0x16, 0x03, 0x03, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04])
    }

    func testTransparentTLSClientWritesClientHelloWithoutConnectPrefix() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let expected = Phase2TLSIngressVectors.http2ClientHello
        let peer = Phase2LoopbackBytePeer(
            group: group,
            configuration: .init(
                expectedBytes: expected,
                maximumInboundBytes: Phase2TLSIngressVectors.maximumVectorBytes
            )
        )
        let client = Phase2TransparentTLSClient(group: group)
        defer {
            client.stop()
            peer.stop()
        }

        try peer.start()
        try client.connectDirectly(port: peer.localPort, offer: .http2)
        let observation = try peer.observation.wait()

        XCTAssertEqual(try observation.futureResult.wait(), expected)
        XCTAssertEqual(expected.first, 0x16)
        XCTAssertFalse(expected.starts(with: Array("CONNECT ".utf8)))
        XCTAssertTrue(Phase2TLSIngressVectors.malformed.allSatisfy {
            $0.count <= Phase2TLSIngressVectors.maximumVectorBytes
        })
    }

    func testOpaqueTCPScenariosAreUniqueBoundedAndExact() {
        XCTAssertEqual(Set(Phase2OpaqueTCPScenarios.all.map(\.name)).count, Phase2OpaqueTCPScenarios.all.count)
        for scenario in Phase2OpaqueTCPScenarios.all {
            XCTAssertLessThanOrEqual(scenario.clientInitialBytes.count, Phase2OpaqueTCPScenario.maximumPayloadBytes)
            XCTAssertLessThanOrEqual(scenario.serverInitialBytes.count, Phase2OpaqueTCPScenario.maximumPayloadBytes)
            XCTAssertLessThanOrEqual(scenario.serverReplyBytes.count, Phase2OpaqueTCPScenario.maximumPayloadBytes)
            XCTAssertFalse(scenario.expectedEvents.isEmpty, scenario.name)
        }
        XCTAssertEqual(
            Phase2OpaqueTCPScenarios.clientHalfClose.expectedEvents,
            [.clientBytes([0x43, 0x35]), .clientOutputClosed, .serverBytes([0x53, 0x35])]
        )
        XCTAssertEqual(
            Phase2OpaqueTCPScenarios.serverHalfClose.expectedEvents,
            [.serverBytes([0x53, 0x36]), .serverOutputClosed, .clientBytes([0x43, 0x36])]
        )
        XCTAssertEqual(
            Phase2OpaqueTCPScenarios.stalledReader.serverInitialBytes.count,
            Phase2OpaqueTCPScenario.maximumPayloadBytes
        )
    }

    func testOpaqueScenariosExecuteOverLoopbackWithExactBytes() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        for scenario in Phase2OpaqueTCPScenarios.all {
            let peer = Phase2LoopbackBytePeer(
                group: group,
                configuration: .init(
                    initialBytes: scenario.serverInitialBytes,
                    expectedBytes: scenario.clientInitialBytes,
                    replyBytes: scenario.serverReplyBytes,
                    closesOutputAfterInitialBytes: scenario.name == "server-half-close"
                )
            )
            let client = Phase2LoopbackByteClient(group: group)
            let expectedInboundBytes = scenario.serverInitialBytes + scenario.serverReplyBytes
            try peer.start()
            try client.connect(
                port: peer.localPort,
                autoRead: scenario.name != "stalled-reader",
                expectedInboundBytes: expectedInboundBytes
            )
            if !scenario.clientInitialBytes.isEmpty {
                try client.write(
                    scenario.clientInitialBytes,
                    closeOutput: scenario.name == "client-half-close"
                )
            }
            if scenario.name == "stalled-reader" {
                try client.resumeReads()
            }
            let observation = try peer.observation.wait()
            XCTAssertEqual(try observation.futureResult.wait(), scenario.clientInitialBytes, scenario.name)
            XCTAssertEqual(try client.inboundBytes?.wait(), expectedInboundBytes, scenario.name)
            client.stop()
            peer.stop()
        }
    }

    func testHTTP1WebSocketPeerAcceptsCoalescedHandshakeAndFrameExactly() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let clientFrame: [UInt8] = [0x81, 0x82, 0x01, 0x02, 0x03, 0x04, 0x69, 0x6B]
        let serverFrame: [UInt8] = [0x81, 0x02, 0x6F, 0x6B]
        let peer = Phase2HTTP1WebSocketPeer(
            group: group,
            expectedClientFrame: clientFrame,
            serverFrame: serverFrame
        )
        let client = Phase2LoopbackByteClient(group: group)
        defer {
            client.stop()
            peer.stop()
        }

        try peer.start()
        try client.connect(port: peer.localPort)
        try client.write(Phase2HTTP1WebSocketPeer.requestBytes + clientFrame)

        XCTAssertEqual(try peer.receivedFrame.wait(), clientFrame)
        XCTAssertEqual(Array(Phase2HTTP1WebSocketPeer.responseBytes.prefix(12)), Array("HTTP/1.1 101".utf8))
    }

    func testHTTP2WebSocketPeerAdvertisesAndEnforcesExtendedConnectExchange() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let expectedData: [UInt8] = [0x81, 0x02, 0x68, 0x32]
        let responseData: [UInt8] = [0x88, 0x00]
        let peer = Phase2HTTP2WebSocketPeer(
            group: group,
            expectedData: expectedData,
            responseData: responseData
        )
        let client = Phase2HTTP2WebSocketClient(
            group: group,
            requestData: expectedData,
            expectedResponseData: responseData
        )
        defer {
            client.stop()
            peer.stop()
        }
        try peer.start()
        try client.connectAndExchange(port: peer.localPort)

        XCTAssertEqual(try peer.exchange.wait().headers.map(\.0), Phase2HTTP2WebSocketPeer.expectedHeaders.map(\.0))
        XCTAssertEqual(try client.response.wait(), responseData)
    }
}

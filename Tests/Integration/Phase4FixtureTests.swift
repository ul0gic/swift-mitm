import NIOCore
import NIOPosix
import XCTest

@testable import SwiftMITM

private struct Phase4TLSVectorExpectation {
    let mode: Phase4TLSClientMode
    let serverName: String?
    let encryptedClientHello: Bool
}

final class Phase4FixtureTests: XCTestCase {
    func testProxyV2ForwarderPreservesGoldenHeaderAndPayloadAcrossDeliveryModes() throws {
        let ipv4 = Phase4ProxyV2Header.ipv4(
            source: [0xC0, 0x00, 0x02, 0x0A],
            destination: [0xC6, 0x33, 0x64, 0x14],
            sourcePort: 12_345,
            destinationPort: 443,
            tlvs: [0x01, 0x00, 0x02, 0x68, 0x32]
        )
        let ipv6 = Phase4ProxyV2Header.ipv6(
            source: [0x20, 0x01, 0x0D, 0xB8] + Array(repeating: 0, count: 11) + [0x01],
            destination: [0x20, 0x01, 0x0D, 0xB8] + Array(repeating: 0, count: 11) + [0x02],
            sourcePort: 8_080,
            destinationPort: 8_443
        )

        XCTAssertEqual(ipv4.bytes, Phase2ProxyV2Corpus.ipv4.bytes)
        XCTAssertEqual(ipv6.bytes, Phase2ProxyV2Corpus.ipv6.bytes)
        try assertForwarded(header: ipv4, delivery: .coalesced)
        try assertForwarded(header: ipv6, delivery: .fragmented([1, 11, 4, 20, 16, 9]))
    }

    func testDirectTLSIngressVectorsCarryExpectedIdentityWithoutConnect() throws {
        let vectors = [
            Phase4TLSVectorExpectation(
                mode: .dnsSNI("localhost"),
                serverName: "localhost",
                encryptedClientHello: false
            ),
            Phase4TLSVectorExpectation(mode: .noSNI, serverName: nil, encryptedClientHello: false),
            Phase4TLSVectorExpectation(mode: .ipIdentity("127.0.0.1"), serverName: nil, encryptedClientHello: false),
            Phase4TLSVectorExpectation(mode: .encryptedClientHello, serverName: "localhost", encryptedClientHello: true)
        ]

        for vector in vectors {
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            let client = Phase4DirectTLSClient(group: group)
            let bytes = client.rawClientHello(for: vector.mode)
            defer {
                client.stop()
                try? group.syncShutdownGracefully()
            }

            let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(bytes))
            XCTAssertEqual(metadata.serverName, vector.serverName)
            XCTAssertEqual(metadata.encryptedClientHelloDetected, vector.encryptedClientHello)
            XCTAssertEqual(bytes.first, 0x16)
            XCTAssertFalse(bytes.starts(with: Array("CONNECT ".utf8)))
            XCTAssertLessThanOrEqual(bytes.count, Phase2TLSIngressVectors.maximumVectorBytes)
            try assertDirectTLSBytes(bytes, mode: vector.mode, group: group, client: client)
        }
    }

    func testClearHTTPAndWebSocketFixturesExchangeExactBytes() throws {
        try assertClearExchange(
            scenario: .requestResponse,
            expectedRequest: Phase4ClearHTTPOrigin.requestBytes,
            expectedResponse: Phase4ClearHTTPOrigin.responseBytes,
            expectedClientFrames: []
        )
        try assertClearExchange(
            scenario: .webSocket,
            expectedRequest: Phase2HTTP1WebSocketPeer.requestBytes,
            expectedResponse: Phase2HTTP1WebSocketPeer.responseBytes + WebSocketWire.serverFrames,
            expectedClientFrames: WebSocketWire.clientFrames
        )
    }

    func testOpaqueFixtureMatrixExecutesExactBytesAndTeardownScenarios() throws {
        XCTAssertEqual(Phase4OpaqueScenarios.all.count, 8)
        XCTAssertEqual(Set(Phase4OpaqueScenarios.all.map(\.name)).count, Phase4OpaqueScenarios.all.count)
        for scenario in Phase4OpaqueScenarios.all {
            XCTAssertLessThanOrEqual(scenario.clientBytes.count, Phase2OpaqueTCPScenario.maximumPayloadBytes)
            XCTAssertLessThanOrEqual(scenario.serverBytes.count, Phase2OpaqueTCPScenario.maximumPayloadBytes)
            XCTAssertLessThanOrEqual(scenario.serverReplyBytes.count, Phase2OpaqueTCPScenario.maximumPayloadBytes)
            try assertOpaqueExchange(scenario)
        }
    }

    private func assertForwarded(header: Phase4ProxyV2Header, delivery: Phase4ProxyV2Delivery) throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let payload = Phase2ProxyV2Corpus.replay
        let expected = header.bytes + payload
        let peer = Phase2LoopbackBytePeer(
            group: group,
            configuration: .init(expectedBytes: expected, maximumInboundBytes: expected.count)
        )
        let forwarder = Phase4ProxyV2Forwarder(group: group)
        defer {
            forwarder.stop()
            peer.stop()
            try? group.syncShutdownGracefully()
        }

        try peer.start()
        _ = try forwarder.connect(listenerPort: peer.localPort)
        try forwarder.send(header: header, applicationBytes: payload, delivery: delivery)
        let observation = try peer.observation.wait()

        XCTAssertEqual(try observation.futureResult.wait(), expected)
        XCTAssertEqual(forwarder.actualPeerAddress?.ipAddress, "127.0.0.1")
    }

    private func assertDirectTLSBytes(
        _ expected: [UInt8],
        mode: Phase4TLSClientMode,
        group: EventLoopGroup,
        client: Phase4DirectTLSClient
    ) throws {
        let peer = Phase2LoopbackBytePeer(
            group: group,
            configuration: .init(
                expectedBytes: expected,
                maximumInboundBytes: Phase2TLSIngressVectors.maximumVectorBytes
            )
        )
        defer { peer.stop() }

        try peer.start()
        try client.sendRawClientHello(port: peer.localPort, mode: mode)
        let observation = try peer.observation.wait()
        XCTAssertEqual(try observation.futureResult.wait(), expected)
    }

    private func assertClearExchange(
        scenario: Phase4ClearHTTPScenario,
        expectedRequest: [UInt8],
        expectedResponse: [UInt8],
        expectedClientFrames: [UInt8]
    ) throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let origin = Phase4ClearHTTPOrigin(group: group, scenario: scenario)
        let client = Phase4ClearHTTPClient(group: group)
        defer {
            client.stop()
            origin.stop()
            try? group.syncShutdownGracefully()
        }

        try origin.start()
        let response = try client.exchange(port: origin.localPort, scenario: scenario)
        let result = try origin.result.wait()

        XCTAssertEqual(try response.wait(), expectedResponse)
        XCTAssertEqual(result.requestBytes, expectedRequest)
        XCTAssertEqual(result.clientWebSocketFrames, expectedClientFrames)
        XCTAssertFalse(result.requestBytes.starts(with: Array("CONNECT ".utf8)))
    }

    private func assertOpaqueExchange(_ scenario: Phase4OpaqueScenario) throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let peer = Phase4OpaquePeer(group: group, scenario: scenario)
        let client = Phase4OpaqueClient(group: group)
        defer {
            client.stop()
            peer.stop()
            try? group.syncShutdownGracefully()
        }

        try peer.start()
        try client.connect(port: peer.localPort, scenario: scenario)
        try peer.accepted.wait()
        if scenario.clientStartsStalled {
            XCTAssertEqual(client.observedInboundBytes, [])
            try client.resumeReads()
        }

        XCTAssertEqual(try peer.receivedBytes.wait(), scenario.clientBytes, scenario.name)
        XCTAssertEqual(
            try client.inboundBytes?.wait(),
            scenario.serverBytes + scenario.serverReplyBytes,
            scenario.name
        )
        XCTAssertEqual(peer.terminal.snapshot.errorCount, 0, scenario.name)
        XCTAssertEqual(client.terminal.snapshot.errorCount, 0, scenario.name)
    }
}

import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class TrustedProxyV2IngressHandlerTests: XCTestCase {
    private enum ExpectedError: Error {
        case acceptanceFailed
    }

    func testTrustedHeaderMapsMetadataAndReplaysOnlyApplicationBytesOnce() throws {
        let accepted = AcceptanceStorage()
        let channel = try makeChannel(accepted: accepted)
        defer { XCTAssertNoThrow(try channel.finish()) }
        let applicationBytes = [UInt8(0x16), 0x03, 0x03, 0x00, 0x02, 0xAA, 0xBB]
        let input = ByteBuffer(bytes: makeIPv4Header() + applicationBytes)

        assertFull(try channel.writeInbound(input))

        let result = try XCTUnwrap(accepted.value)
        XCTAssertEqual(result.metadata.sourceAddress.ipAddress, "192.0.2.10")
        XCTAssertEqual(result.target.connectionHost, "198.51.100.20")
        XCTAssertEqual(result.target.port, 443)
        XCTAssertEqual(result.target.logicalAuthority, "198.51.100.20:443")
        XCTAssertNil(result.target.tlsServerName)
        XCTAssertEqual(result.target.leafIdentity, "198.51.100.20")
        let replay = try XCTUnwrap(channel.readInbound(as: ByteBuffer.self))
        XCTAssertEqual(Array(replay.readableBytesView), applicationBytes)
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))

        assertFull(try channel.writeInbound(ByteBuffer(bytes: [0x01, 0x02])))
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), [0x01, 0x02])
        XCTAssertEqual(accepted.count, 1)
    }

    func testEveryHeaderChunkBoundaryReplaysApplicationBytesExactlyOnce() throws {
        let header = makeIPv4Header()
        let applicationBytes = [UInt8(0x47), 0x45, 0x54]

        for boundary in 0..<header.count {
            let accepted = AcceptanceStorage()
            let channel = try makeChannel(accepted: accepted)
            defer { _ = try? channel.finish() }

            if boundary > 0 {
                assertEmpty(try channel.writeInbound(ByteBuffer(bytes: header.prefix(boundary))))
            }
            assertFull(try channel.writeInbound(ByteBuffer(bytes: header.dropFirst(boundary) + applicationBytes)))

            XCTAssertEqual(accepted.count, 1, "boundary \(boundary)")
            let replay = try XCTUnwrap(channel.readInbound(as: ByteBuffer.self))
            XCTAssertEqual(Array(replay.readableBytesView), applicationBytes, "boundary \(boundary)")
            XCTAssertNil(try channel.readInbound(as: ByteBuffer.self), "boundary \(boundary)")
        }
    }

    func testUntrustedAndUnavailablePeersTerminateBeforeParsing() throws {
        for (peer, expectedError) in [
            (try SocketAddress(ipAddress: "192.0.2.1", port: 1000), TrustedProxyV2IngressError.untrustedPeer),
            (nil, TrustedProxyV2IngressError.peerAddressUnavailable)
        ] as [(SocketAddress?, TrustedProxyV2IngressError)] {
            let errors = ErrorStorage()
            let channel = try makeChannel(peer: peer, errors: errors)
            channel.embeddedEventLoop.run()

            XCTAssertFalse(channel.isActive)
            XCTAssertEqual(errors.ingressErrors, [expectedError])
            XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
        }
    }

    func testMalformedAndOversizedHeadersTerminateWithoutAcceptance() throws {
        let accepted = AcceptanceStorage()
        let errors = ErrorStorage()
        let channel = try makeChannel(accepted: accepted, errors: errors)

        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: Array("PROXY TCP4".utf8))))
        channel.embeddedEventLoop.run()

        XCTAssertFalse(channel.isActive)
        XCTAssertEqual(errors.parserErrors, [.invalidSignature])
        XCTAssertNil(accepted.value)
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))

        let smallConfiguration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            proxyHeaderMaximumBytes: 28
        ))
        let oversizedErrors = ErrorStorage()
        let oversized = try makeChannel(configuration: smallConfiguration, errors: oversizedErrors)
        assertEmpty(try oversized.writeInbound(ByteBuffer(bytes: makeIPv4Header(tlv: [0x01, 0, 1, 0xAA]))))
        oversized.embeddedEventLoop.run()

        XCTAssertFalse(oversized.isActive)
        XCTAssertEqual(oversizedErrors.parserErrors, [.headerTooLarge(declared: 32, maximum: 28)])
        XCTAssertNoThrow(try oversized.finish(acceptAlreadyClosed: true))
    }

    func testIncompleteHeaderDeadlineTerminatesOnce() throws {
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            proxyHeaderDeadline: .seconds(5)
        ))
        let errors = ErrorStorage()
        let channel = try makeChannel(configuration: configuration, errors: errors)

        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: makeIPv4Header().prefix(8))))
        channel.embeddedEventLoop.advanceTime(by: .seconds(5))

        XCTAssertFalse(channel.isActive)
        XCTAssertEqual(errors.ingressErrors, [.deadlineExceeded])
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testCloseBeforeCompletionReportsTruncatedHeader() throws {
        let errors = ErrorStorage()
        let channel = try makeChannel(errors: errors)
        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: makeIPv4Header().prefix(8))))

        try channel.close().wait()

        XCTAssertEqual(errors.parserErrors, [.truncatedHeader])
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testAcceptanceFailureClosesWithoutReplayingApplicationBytes() throws {
        let errors = ErrorStorage()
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        let channel = EmbeddedChannel()
        try channel.pipeline.syncOperations.addHandler(errors)
        let handler = TrustedProxyV2IngressHandler(
            configuration: configuration,
            peerAddressProvider: { _ in try? SocketAddress(ipAddress: "127.0.0.1", port: 1000) },
            acceptanceHandler: { _, _ in throw ExpectedError.acceptanceFailed }
        )
        try channel.pipeline.syncOperations.addHandler(handler, position: .first)

        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: makeIPv4Header() + [0xAA])))
        channel.embeddedEventLoop.run()

        XCTAssertFalse(channel.isActive)
        XCTAssertTrue(errors.errors.contains { $0 is ExpectedError })
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    private func makeChannel(
        configuration: TrustedProxyV2Ingress? = TrustedProxyV2Ingress(trustedPeers: .loopback),
        peer: SocketAddress? = try? SocketAddress(ipAddress: "127.0.0.1", port: 1000),
        accepted: AcceptanceStorage = AcceptanceStorage(),
        errors: ErrorStorage = ErrorStorage()
    ) throws -> EmbeddedChannel {
        let configuration = try XCTUnwrap(configuration)
        let channel = EmbeddedChannel()
        try channel.pipeline.syncOperations.addHandler(errors)
        let handler = TrustedProxyV2IngressHandler(
            configuration: configuration,
            peerAddressProvider: { _ in peer },
            acceptanceHandler: { _, result in accepted.record(result) }
        )
        try channel.pipeline.syncOperations.addHandler(handler, position: .first)
        return channel
    }

    private func makeIPv4Header(tlv: [UInt8] = []) -> [UInt8] {
        let addressBlock = [UInt8(192), 0, 2, 10, 198, 51, 100, 20]
            + encodedUInt16(12_345) + encodedUInt16(443) + tlv
        return [0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x21, 0x11]
            + encodedUInt16(addressBlock.count) + addressBlock
    }

    private func encodedUInt16(_ value: Int) -> [UInt8] {
        [UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }

    private func assertEmpty(
        _ state: EmbeddedChannel.BufferState,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case .empty = state else {
            return XCTFail("expected empty buffer", file: file, line: line)
        }
    }

    private func assertFull(
        _ state: EmbeddedChannel.BufferState,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case .full = state else {
            return XCTFail("expected full buffer", file: file, line: line)
        }
    }
}

private final class AcceptanceStorage {
    private(set) var value: TrustedProxyV2AcceptedConnection?
    private(set) var count = 0

    func record(_ value: TrustedProxyV2AcceptedConnection) {
        self.value = value
        count += 1
    }
}

private final class ErrorStorage: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private(set) var errors: [Error] = []

    var ingressErrors: [TrustedProxyV2IngressError] {
        errors.compactMap { $0 as? TrustedProxyV2IngressError }
    }

    var parserErrors: [ProxyV2ParserError] {
        errors.compactMap { $0 as? ProxyV2ParserError }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        errors.append(error)
    }
}

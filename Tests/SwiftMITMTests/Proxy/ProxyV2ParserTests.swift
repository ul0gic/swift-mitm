import NIOCore
import XCTest

@testable import SwiftMITM

final class ProxyV2ParserTests: XCTestCase {
    func testOwningBoundaryUsesFiveSecondIncompleteHeaderDeadline() {
        XCTAssertEqual(ProxyV2ParserBoundary.deadline, .seconds(5))
    }

    func testIPv4HeaderParsesSourceDestinationAndTLVs() throws {
        let header = makeIPv4Header(
            source: [192, 0, 2, 10],
            destination: [198, 51, 100, 20],
            sourcePort: 12_345,
            destinationPort: 443,
            tlvs: [[0x01, 0x00, 0x02, 0x68, 0x32], [0xEE, 0x00, 0x00]]
        )
        var input = ByteBuffer(bytes: header)
        var parser = ProxyV2Parser()

        guard case .complete(let metadata) = try parser.parse(&input) else {
            return XCTFail("expected complete metadata")
        }

        XCTAssertEqual(metadata.sourceAddress.ipAddress, "192.0.2.10")
        XCTAssertEqual(metadata.sourceAddress.port, 12_345)
        XCTAssertEqual(metadata.destinationAddress.ipAddress, "198.51.100.20")
        XCTAssertEqual(metadata.destinationAddress.port, 443)
        XCTAssertEqual(metadata.tlvCount, 2)
        XCTAssertEqual(input.readableBytes, 0)
        XCTAssertNoThrow(try parser.finish())
    }

    func testIPv6HeaderParsesSourceAndDestination() throws {
        let source = Array(repeating: UInt8(0), count: 15) + [1]
        let destination = [0x20, 0x01, 0x0D, 0xB8] + Array(repeating: UInt8(0), count: 11) + [2]
        var input = ByteBuffer(bytes: makeIPv6Header(
            source: source,
            destination: destination,
            sourcePort: 8_080,
            destinationPort: 8443
        ))
        var parser = ProxyV2Parser()

        guard case .complete(let metadata) = try parser.parse(&input) else {
            return XCTFail("expected complete metadata")
        }

        XCTAssertEqual(metadata.sourceAddress.ipAddress, "::1")
        XCTAssertEqual(metadata.sourceAddress.port, 8_080)
        XCTAssertEqual(metadata.destinationAddress.ipAddress, "2001:db8::2")
        XCTAssertEqual(metadata.destinationAddress.port, 8443)
    }

    func testApplicationBytesRemainUnreadAtEveryHeaderChunkBoundary() throws {
        let header = makeIPv4Header(tlvs: [[0x01, 0x00, 0x03, 0x01, 0x02, 0x03]])
        let applicationBytes: [UInt8] = [0x16, 0x03, 0x03, 0x00, 0x08, 0xAA, 0xBB]

        for splitIndex in 0 ..< header.count {
            var parser = ProxyV2Parser()
            var first = ByteBuffer(bytes: header.prefix(splitIndex))
            XCTAssertEqual(try parser.parse(&first), .pending, "split \(splitIndex)")
            XCTAssertEqual(first.readableBytes, 0, "split \(splitIndex)")

            var second = ByteBuffer(bytes: header.dropFirst(splitIndex) + applicationBytes)
            guard case .complete = try parser.parse(&second) else {
                return XCTFail("expected completion at split \(splitIndex)")
            }
            XCTAssertTrue(second.readableBytesView.elementsEqual(applicationBytes), "split \(splitIndex)")
        }
    }

    func testOneByteChunksCompleteWithoutRetainingApplicationBytes() throws {
        let header = makeIPv4Header()
        var parser = ProxyV2Parser()
        for byte in header.dropLast() {
            var input = ByteBuffer(bytes: [byte])
            XCTAssertEqual(try parser.parse(&input), .pending)
        }
        var final = ByteBuffer(bytes: [header[header.count - 1], 0x47, 0x45, 0x54])

        guard case .complete = try parser.parse(&final) else { return XCTFail("expected completion") }
        XCTAssertTrue(final.readableBytesView.elementsEqual([0x47, 0x45, 0x54]))
    }

    func testMaximumSizedHeaderParsesWithoutRetainingTLVPayload() throws {
        let tlvValueByteCount = ProxyV2Parser.defaultMaximumHeaderBytes - 16 - 12 - 3
        let tlv = [UInt8(0xEE)] + encodedUInt16(tlvValueByteCount)
            + Array(repeating: UInt8(0xA5), count: tlvValueByteCount)
        var input = ByteBuffer(bytes: makeIPv4Header(tlvs: [tlv]))
        var parser = ProxyV2Parser()

        guard case .complete(let metadata) = try parser.parse(&input) else {
            return XCTFail("expected complete metadata")
        }
        XCTAssertEqual(metadata.tlvCount, 1)
    }

    func testRejectsInvalidSignatureAndV1TextImmediately() {
        assertError(.invalidSignature, bytes: Array("PROXY TCP4".utf8))
        assertError(.invalidSignature, bytes: [0x0D, 0x0A, 0x0D, 0x0B])
    }

    func testRejectsUnsupportedVersionCommandFamilyAndTransport() {
        assertError(.unsupportedVersion(1), bytes: makeIPv4Header(versionAndCommand: 0x11))
        assertError(.unsupportedCommand(0), bytes: makeIPv4Header(versionAndCommand: 0x20))
        assertError(.unsupportedAddressFamily(0), bytes: makeIPv4Header(familyAndTransport: 0x01))
        assertError(.unsupportedAddressFamily(3), bytes: makeIPv4Header(familyAndTransport: 0x31))
        assertError(.unsupportedTransport(2), bytes: makeIPv4Header(familyAndTransport: 0x12))
    }

    func testRejectsShortAddressBlocksAndZeroPorts() {
        assertError(.malformedAddressBlock, bytes: makeHeader(addressBlock: Array(repeating: 0, count: 11)))
        assertError(.invalidPort, bytes: makeIPv4Header(sourcePort: 0))
        assertError(.invalidPort, bytes: makeIPv4Header(destinationPort: 0))
        assertError(.invalidPort, bytes: makeIPv6Header(sourcePort: 0))
        assertError(.invalidPort, bytes: makeIPv6Header(destinationPort: 0))
    }

    func testRejectsMalformedTLVs() {
        assertError(.malformedTLV, bytes: makeIPv4Header(tlvs: [[0x01]]))
        assertError(.malformedTLV, bytes: makeIPv4Header(tlvs: [[0x01, 0x00]]))
        assertError(.malformedTLV, bytes: makeIPv4Header(tlvs: [[0x01, 0x00, 0x02, 0xAA]]))
    }

    func testRejectsHeaderAboveConfiguredLimitAfterFixedHeader() {
        let bytes = makeIPv4Header(tlvs: [[0x01, 0x00, 0x01, 0xAA]])
        var input = ByteBuffer(bytes: bytes)
        var parser = ProxyV2Parser(maximumHeaderBytes: 28)

        XCTAssertThrowsError(try parser.parse(&input)) { error in
            XCTAssertEqual(error as? ProxyV2ParserError, .headerTooLarge(declared: 32, maximum: 28))
        }
        XCTAssertEqual(input.readerIndex, 16)
    }

    func testFinishRejectsEveryTruncatedHeaderBoundary() throws {
        let header = makeIPv6Header(tlvs: [[0x01, 0x00, 0x01, 0xAA]])
        for byteCount in 0 ..< header.count {
            var input = ByteBuffer(bytes: header.prefix(byteCount))
            var parser = ProxyV2Parser()
            XCTAssertEqual(try parser.parse(&input), .pending, "byte count \(byteCount)")
            XCTAssertThrowsError(try parser.finish()) { error in
                XCTAssertEqual(error as? ProxyV2ParserError, .truncatedHeader)
            }
        }
    }

    func testParserCannotEmitCompletionTwice() throws {
        var input = ByteBuffer(bytes: makeIPv4Header())
        var parser = ProxyV2Parser()
        guard case .complete = try parser.parse(&input) else { return XCTFail("expected completion") }
        var extra = ByteBuffer(bytes: [0x01])

        XCTAssertThrowsError(try parser.parse(&extra)) { error in
            XCTAssertEqual(error as? ProxyV2ParserError, .parserAlreadyCompleted)
        }
        XCTAssertEqual(extra.readerIndex, 0)
    }

    private func assertError(
        _ expectedError: ProxyV2ParserError,
        bytes: [UInt8],
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        var input = ByteBuffer(bytes: bytes)
        var parser = ProxyV2Parser()
        XCTAssertThrowsError(try parser.parse(&input), file: file, line: line) { error in
            XCTAssertEqual(error as? ProxyV2ParserError, expectedError, file: file, line: line)
        }
    }

    private func makeIPv4Header(
        versionAndCommand: UInt8 = 0x21,
        familyAndTransport: UInt8 = 0x11,
        source: [UInt8] = [192, 0, 2, 1],
        destination: [UInt8] = [198, 51, 100, 1],
        sourcePort: Int = 12_345,
        destinationPort: Int = 443,
        tlvs: [[UInt8]] = []
    ) -> [UInt8] {
        makeHeader(
            versionAndCommand: versionAndCommand,
            familyAndTransport: familyAndTransport,
            addressBlock: source + destination + encodedUInt16(sourcePort) + encodedUInt16(destinationPort),
            tlvs: tlvs
        )
    }

    private func makeIPv6Header(
        source: [UInt8] = Array(repeating: 0, count: 15) + [1],
        destination: [UInt8] = [0x20, 0x01, 0x0D, 0xB8] + Array(repeating: 0, count: 11) + [1],
        sourcePort: Int = 12_345,
        destinationPort: Int = 443,
        tlvs: [[UInt8]] = []
    ) -> [UInt8] {
        makeHeader(
            familyAndTransport: 0x21,
            addressBlock: source + destination + encodedUInt16(sourcePort) + encodedUInt16(destinationPort),
            tlvs: tlvs
        )
    }

    private func makeHeader(
        versionAndCommand: UInt8 = 0x21,
        familyAndTransport: UInt8 = 0x11,
        addressBlock: [UInt8],
        tlvs: [[UInt8]] = []
    ) -> [UInt8] {
        let payload = addressBlock + tlvs.flatMap(\.self)
        return [0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A]
            + [versionAndCommand, familyAndTransport]
            + encodedUInt16(payload.count)
            + payload
    }

    private func encodedUInt16(_ value: Int) -> [UInt8] {
        [UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }
}

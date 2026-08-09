import XCTest

@testable import SwiftMITM

final class ClientHelloALPNParserTests: XCTestCase {
    func testFragmentedTLSRecordsProduceOrderedDeduplicatedSupportedProtocols() throws {
        let handshake = makeClientHello(protocols: ["http/1.1", "h2", "h2", "unsupported"])
        let records = makeTLSRecords(handshake: handshake, payloadLengths: [2, 11])

        guard case .protocols(let protocols)? = try ClientHelloALPNParser.parse(records) else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertEqual(protocols, [.http11, .http2])
    }

    func testClientHelloWithoutALPNReturnsAbsent() throws {
        let bytes = makeTLSRecords(handshake: makeClientHello(protocols: nil))

        guard case .absent? = try ClientHelloALPNParser.parse(bytes) else {
            return XCTFail("expected absent ALPN")
        }
    }

    func testUnsupportedOnlyOfferReturnsEmptySupportedProtocols() throws {
        let bytes = makeTLSRecords(handshake: makeClientHello(protocols: ["acme/1", "acme/2"]))

        guard case .protocols(let protocols)? = try ClientHelloALPNParser.parse(bytes) else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertTrue(protocols.isEmpty)
    }

    func testTruncatedTLSRecordRemainsPending() throws {
        let complete = makeTLSRecords(handshake: makeClientHello(protocols: ["h2"]))

        XCTAssertNil(try ClientHelloALPNParser.parse(Array(complete.dropLast())))
    }

    func testMalformedALPNVectorLengthFails() {
        let bytes = makeTLSRecords(
            handshake: makeClientHello(protocols: ["h2"], declaredALPNListLength: 4)
        )

        XCTAssertThrowsError(try ClientHelloALPNParser.parse(bytes))
    }

    func testMalformedExtensionsLengthFails() {
        let bytes = makeTLSRecords(
            handshake: makeClientHello(protocols: ["h2"], declaredExtensionsLength: 1)
        )

        XCTAssertThrowsError(try ClientHelloALPNParser.parse(bytes))
    }

    func testTruncatedClientHelloVectorFailsAfterCompleteHandshakeArrives() {
        var body = Array(repeating: UInt8(0), count: 34)
        body.append(8)
        body.append(0)
        let handshake = [UInt8(1)] + encodedUInt24(body.count) + body

        XCTAssertThrowsError(try ClientHelloALPNParser.parse(makeTLSRecords(handshake: handshake)))
    }

    func testNonHandshakeRecordFails() {
        XCTAssertThrowsError(try ClientHelloALPNParser.parse([23, 3, 3, 0, 0]))
    }

    func testDeclaredClientHelloAboveLimitFailsBeforeBodyAllocation() {
        let bytes: [UInt8] = [22, 3, 3, 0, 4, 1, 1, 0, 0]

        XCTAssertThrowsError(try ClientHelloALPNParser.parse(bytes))
    }

    private func makeClientHello(
        protocols: [String]?,
        declaredALPNListLength: Int? = nil,
        declaredExtensionsLength: Int? = nil
    ) -> [UInt8] {
        var body: [UInt8] = [3, 3]
        body.append(contentsOf: repeatElement(0, count: 32))
        body.append(0)
        body.append(contentsOf: [0, 2, 0x13, 0x01])
        body.append(contentsOf: [1, 0])

        var extensions: [UInt8] = []
        if let protocols {
            let names = protocols.flatMap { name -> [UInt8] in
                let bytes = Array(name.utf8)
                return [UInt8(bytes.count)] + bytes
            }
            let listLength = declaredALPNListLength ?? names.count
            let alpn = encodedUInt16(listLength) + names
            extensions = [0, 16] + encodedUInt16(alpn.count) + alpn
        }
        body.append(contentsOf: encodedUInt16(declaredExtensionsLength ?? extensions.count))
        body.append(contentsOf: extensions)
        return [1] + encodedUInt24(body.count) + body
    }

    private func makeTLSRecords(handshake: [UInt8], payloadLengths: [Int] = []) -> [UInt8] {
        var records: [UInt8] = []
        var index = 0
        for requestedLength in payloadLengths where index < handshake.count {
            let length = min(requestedLength, handshake.count - index)
            records.append(contentsOf: [22, 3, 3])
            records.append(contentsOf: encodedUInt16(length))
            records.append(contentsOf: handshake[index ..< index + length])
            index += length
        }
        if index < handshake.count {
            let remainder = handshake.count - index
            records.append(contentsOf: [22, 3, 3])
            records.append(contentsOf: encodedUInt16(remainder))
            records.append(contentsOf: handshake[index...])
        }
        return records
    }

    private func encodedUInt16(_ value: Int) -> [UInt8] {
        [UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }

    private func encodedUInt24(_ value: Int) -> [UInt8] {
        [UInt8((value >> 16) & 0xFF), UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }
}

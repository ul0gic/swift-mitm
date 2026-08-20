import XCTest

@testable import SwiftMITM

final class ClientHelloALPNParserTests: XCTestCase {
    func testOwningBoundaryUsesOneSecondInspectionDeadline() {
        XCTAssertEqual(ClientHelloInspectionBoundary.deadline, .seconds(1))
    }

    func testMetadataRetainsAllOfferedProtocolsAndDeduplicatesSupportedProjection() throws {
        let handshake = makeClientHello(protocols: ["http/1.1", "acme/1", "h2", "h2", "http/1.1"])
        let records = makeTLSRecords(handshake: handshake, payloadLengths: [2, 11])

        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(records))

        XCTAssertEqual(
            metadata.offeredALPNProtocols.map(\.bytes),
            ["http/1.1", "acme/1", "h2", "h2", "http/1.1"].map { Array($0.utf8) }
        )
        XCTAssertEqual(metadata.supportedALPNProtocols, [.http11, .http2])
        XCTAssertTrue(metadata.hasALPNExtension)
        guard case .protocols(let protocols) = metadata.compatibilityALPNOffer else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertEqual(protocols, [.http11, .http2])
    }

    func testClientHelloWithoutALPNOrSNIProducesExplicitAbsence() throws {
        let bytes = makeTLSRecords(handshake: makeClientHello(protocols: nil))

        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(bytes))

        XCTAssertFalse(metadata.hasALPNExtension)
        XCTAssertTrue(metadata.offeredALPNProtocols.isEmpty)
        XCTAssertTrue(metadata.supportedALPNProtocols.isEmpty)
        XCTAssertNil(metadata.serverName)
        guard case .absent = metadata.compatibilityALPNOffer else {
            return XCTFail("expected absent ALPN")
        }
    }

    func testDNSNameIsValidatedAndNormalized() throws {
        let bytes = makeTLSRecords(handshake: makeClientHello(
            protocols: ["h2"],
            extensionsBeforeALPN: [makeServerNameExtension(names: [Array("API.Example-1.COM".utf8)])]
        ))

        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(bytes))

        XCTAssertEqual(metadata.serverName, "api.example-1.com")
        XCTAssertFalse(metadata.encryptedClientHelloDetected)
    }

    func testECHIsDetectedIndependentlyOfOuterMetadata() throws {
        let bytes = makeTLSRecords(handshake: makeClientHello(
            protocols: ["h2"],
            extensionsBeforeALPN: [makeExtension(type: 0xFE0D, payload: [0x01, 0x02])]
        ))

        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(bytes))

        XCTAssertTrue(metadata.encryptedClientHelloDetected)
        XCTAssertNil(metadata.serverName)
        XCTAssertEqual(metadata.offeredALPNProtocols.map(\.bytes), [Array("h2".utf8)])
    }

    func testUnsupportedASCIIOnlyOfferPreservesIdentifiersAndEmptyCompatibilityProjection() throws {
        let bytes = makeTLSRecords(handshake: makeClientHello(protocols: ["acme/1", "acme/2"]))

        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(bytes))

        XCTAssertEqual(metadata.offeredALPNProtocols.map(\.bytes), ["acme/1", "acme/2"].map { Array($0.utf8) })
        XCTAssertTrue(metadata.supportedALPNProtocols.isEmpty)
        guard case .protocols(let protocols)? = try ClientHelloALPNParser.parse(bytes) else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertTrue(protocols.isEmpty)
    }

    func testMixedOpaqueAndHTTP2IdentifiersPreserveBytesAndProjectHTTP2() throws {
        let identifiers: [[UInt8]] = [[0xFF, 0x00], Array("h2".utf8), [0x80], Array("h2".utf8)]
        let bytes = makeTLSRecords(handshake: makeClientHello(protocols: nil, rawProtocols: identifiers))

        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(bytes))

        XCTAssertEqual(metadata.offeredALPNProtocols.map(\.bytes), identifiers)
        XCTAssertEqual(metadata.supportedALPNProtocols, [.http2])
        guard case .protocols(let protocols)? = try ClientHelloALPNParser.parse(bytes) else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertEqual(protocols, [.http2])
    }

    func testUnsupportedNonUTF8OnlyOfferReturnsEmptyCompatibilityProjection() throws {
        let identifiers: [[UInt8]] = [[0xFF], [0x80, 0x00, 0xFE]]
        let bytes = makeTLSRecords(handshake: makeClientHello(protocols: nil, rawProtocols: identifiers))

        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(bytes))

        XCTAssertEqual(metadata.offeredALPNProtocols.map(\.bytes), identifiers)
        XCTAssertTrue(metadata.supportedALPNProtocols.isEmpty)
        guard case .protocols(let protocols)? = try ClientHelloALPNParser.parse(bytes) else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertTrue(protocols.isEmpty)
    }

    func testEveryNetworkFragmentBoundaryRemainsPendingUntilComplete() throws {
        let complete = makeTLSRecords(
            handshake: makeClientHello(
                protocols: ["h2", "http/1.1"],
                extensionsBeforeALPN: [makeServerNameExtension(names: [Array("example.com".utf8)])]
            ),
            payloadLengths: [1, 3, 17]
        )

        for byteCount in 0 ..< complete.count {
            XCTAssertNil(
                try ClientHelloALPNParser.inspect(Array(complete.prefix(byteCount))),
                "byte count \(byteCount)"
            )
        }
        let metadata = try XCTUnwrap(ClientHelloALPNParser.inspect(complete))
        XCTAssertEqual(metadata.serverName, "example.com")
        XCTAssertEqual(metadata.supportedALPNProtocols, [.http2, .http11])
    }

    func testMalformedAndIPFormServerNamesFail() {
        let invalidNames: [[UInt8]] = [
            [],
            Array("192.0.2.1".utf8),
            Array("2001:db8::1".utf8),
            Array("example.com.".utf8),
            Array("bad_name.example".utf8),
            Array("-bad.example".utf8),
            Array("bad-.example".utf8),
            [0xFF]
        ]
        for name in invalidNames {
            assertMalformed(extensions: [makeServerNameExtension(names: [name])])
        }
    }

    func testEmptyMalformedAndDuplicateServerNameDataFails() {
        assertMalformed(extensions: [makeExtension(type: 0, payload: [0, 0])])
        assertMalformed(extensions: [makeExtension(type: 0, payload: [0, 1, 0])])
        assertMalformed(extensions: [makeServerNameExtension(names: [Array("a.test".utf8), Array("b.test".utf8)])])
        let serverName = makeServerNameExtension(names: [Array("example.com".utf8)])
        assertMalformed(extensions: [serverName, serverName])
    }

    func testDuplicateALPNAndECHExtensionsFail() {
        let alpn = makeALPNExtension(protocols: ["h2"])
        assertMalformed(extensions: [alpn, alpn])
        let ech = makeExtension(type: 0xFE0D, payload: [1])
        assertMalformed(extensions: [ech, ech])
    }

    func testMalformedALPNVectorLengthsFail() {
        assertMalformed(extensions: [makeExtension(type: 16, payload: [0, 1, 0])])
        assertMalformed(extensions: [makeExtension(type: 16, payload: [0, 2, 2, 0xFF])])
        let bytes = makeTLSRecords(
            handshake: makeClientHello(protocols: ["h2"], declaredALPNListLength: 4)
        )
        XCTAssertThrowsError(try ClientHelloALPNParser.inspect(bytes))
    }

    func testMalformedExtensionsLengthFails() {
        let bytes = makeTLSRecords(
            handshake: makeClientHello(protocols: ["h2"], declaredExtensionsLength: 1)
        )

        XCTAssertThrowsError(try ClientHelloALPNParser.inspect(bytes))
    }

    func testTruncatedClientHelloVectorFailsAfterCompleteHandshakeArrives() {
        var body = Array(repeating: UInt8(0), count: 34)
        body.append(8)
        body.append(0)
        let handshake = [UInt8(1)] + encodedUInt24(body.count) + body

        XCTAssertThrowsError(try ClientHelloALPNParser.inspect(makeTLSRecords(handshake: handshake)))
    }

    func testNonHandshakeRecordFails() {
        XCTAssertThrowsError(try ClientHelloALPNParser.inspect([23]))
        XCTAssertThrowsError(try ClientHelloALPNParser.inspect([23, 3, 3, 0, 0]))
    }

    func testDeclaredAndReceivedClientHelloAboveLimitFail() {
        let declaredTooLarge: [UInt8] = [22, 3, 3, 0, 4, 1, 1, 0, 0]
        XCTAssertThrowsError(try ClientHelloALPNParser.inspect(declaredTooLarge))

        let receivedTooLarge = Array(repeating: UInt8(0), count: ClientHelloALPNParser.maximumClientHelloBytes + 1)
        XCTAssertThrowsError(try ClientHelloALPNParser.inspect(receivedTooLarge))
    }

    private func assertMalformed(
        extensions: [[UInt8]],
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        let bytes = makeTLSRecords(handshake: makeClientHello(protocols: nil, extensionsBeforeALPN: extensions))
        XCTAssertThrowsError(try ClientHelloALPNParser.inspect(bytes), file: file, line: line)
    }

    private func makeClientHello(
        protocols: [String]?,
        rawProtocols: [[UInt8]]? = nil,
        extensionsBeforeALPN: [[UInt8]] = [],
        declaredALPNListLength: Int? = nil,
        declaredExtensionsLength: Int? = nil
    ) -> [UInt8] {
        var body: [UInt8] = [3, 3]
        body.append(contentsOf: repeatElement(0, count: 32))
        body.append(0)
        body.append(contentsOf: [0, 2, 0x13, 0x01])
        body.append(contentsOf: [1, 0])

        var extensions = extensionsBeforeALPN.flatMap(\.self)
        let protocolIdentifiers = rawProtocols ?? protocols?.map { Array($0.utf8) }
        if let protocolIdentifiers {
            extensions.append(contentsOf: makeALPNExtension(
                identifiers: protocolIdentifiers,
                declaredListLength: declaredALPNListLength
            ))
        }
        body.append(contentsOf: encodedUInt16(declaredExtensionsLength ?? extensions.count))
        body.append(contentsOf: extensions)
        return [1] + encodedUInt24(body.count) + body
    }

    private func makeALPNExtension(protocols: [String], declaredListLength: Int? = nil) -> [UInt8] {
        makeALPNExtension(
            identifiers: protocols.map { Array($0.utf8) },
            declaredListLength: declaredListLength
        )
    }

    private func makeALPNExtension(
        identifiers: [[UInt8]],
        declaredListLength: Int? = nil
    ) -> [UInt8] {
        let names = identifiers.flatMap { [UInt8($0.count)] + $0 }
        let payload = encodedUInt16(declaredListLength ?? names.count) + names
        return makeExtension(type: 16, payload: payload)
    }

    private func makeServerNameExtension(names: [[UInt8]]) -> [UInt8] {
        let entries = names.flatMap { name in [UInt8(0)] + encodedUInt16(name.count) + name }
        return makeExtension(type: 0, payload: encodedUInt16(entries.count) + entries)
    }

    private func makeExtension(type: Int, payload: [UInt8]) -> [UInt8] {
        encodedUInt16(type) + encodedUInt16(payload.count) + payload
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

import NIOCore
import XCTest

@testable import SwiftMITM

final class Phase5ProxyV2ConformanceTests: XCTestCase {
    func testVersionedArtifactHasCanonicalSchemaAndEmitterProfiles() throws {
        let document = try Phase5ProxyV2ConformanceDocument.load()
        let emitterVectors = document.vectors.filter { $0.roles.contains(.emitter) }

        XCTAssertEqual(document.format, "swiftmitm.proxy-v2-conformance")
        XCTAssertEqual(document.schemaVersion, 1)
        XCTAssertEqual(document.contractVersion, "1.0.0")
        XCTAssertEqual(document.maximumHeaderBytes, ProxyV2Parser.defaultMaximumHeaderBytes)
        XCTAssertEqual(Set(emitterVectors.map(\.id)), ["tcp4-minimal", "tcp6-minimal"])
        for vector in emitterVectors {
            let source = try XCTUnwrap(vector.source)
            let destination = try XCTUnwrap(vector.destination)
            XCTAssertEqual(
                try Phase5ProxyV2Encoder.encode(
                    source: source,
                    destination: destination,
                    tlvs: vector.tlvBytes
                ),
                try vector.headerBytes,
                vector.id
            )
        }
    }

    func testAcceptedVectorsParseAcrossEveryBoundaryAndReplayApplicationBytesOnce() throws {
        let vectors = try Phase5ProxyV2ConformanceDocument.load().vectors.filter { $0.disposition == .accept }

        for vector in vectors {
            try assertAccepted(vector)
        }
    }

    func testRejectedVectorsCoverEveryReceiverPolicyCategory() throws {
        let vectors = try Phase5ProxyV2ConformanceDocument.load().vectors.filter { $0.disposition == .reject }
        let reasons = Set(try vectors.map { vector in
            var input = ByteBuffer(bytes: try vector.headerBytes)
            var parser = ProxyV2Parser()
            XCTAssertThrowsError(try parser.parse(&input), vector.id) { error in
                XCTAssertEqual(self.reason(for: error as? ProxyV2ParserError), vector.reason, vector.id)
            }
            return try XCTUnwrap(vector.reason)
        })

        XCTAssertEqual(reasons, [
            "header-too-large",
            "invalid-port",
            "invalid-signature",
            "malformed-address-block",
            "malformed-tlv",
            "unsupported-address-family",
            "unsupported-command",
            "unsupported-transport",
            "unsupported-version"
        ])
    }

    func testArtifactRetainsPhase2AndPhase4IndependentGoldenOracles() throws {
        let document = try Phase5ProxyV2ConformanceDocument.load()
        let tlv = try XCTUnwrap(document.vectors.first { $0.id == "tcp4-well-formed-tlv" })
        let ipv6 = try XCTUnwrap(document.vectors.first { $0.id == "tcp6-minimal" })
        let source = try XCTUnwrap(tlv.source)
        let destination = try XCTUnwrap(tlv.destination)

        XCTAssertEqual(try tlv.headerBytes, Phase2ProxyV2Corpus.ipv4.bytes)
        XCTAssertEqual(try ipv6.headerBytes, Phase2ProxyV2Corpus.ipv6.bytes)
        XCTAssertEqual(try tlv.applicationBytes, Phase2ProxyV2Corpus.replay)
        XCTAssertEqual(
            Phase4ProxyV2Header.ipv4(
                source: [192, 0, 2, 10],
                destination: [198, 51, 100, 20],
                sourcePort: source.port,
                destinationPort: destination.port,
                tlvs: try tlv.tlvBytes
            ).bytes,
            try tlv.headerBytes
        )
    }

    private func assertAccepted(_ vector: Phase5ProxyV2ConformanceVector) throws {
        let header = try vector.headerBytes
        let application = try vector.applicationBytes
        let source = try XCTUnwrap(vector.source)
        let destination = try XCTUnwrap(vector.destination)

        for splitIndex in 0 ..< header.count {
            var parser = ProxyV2Parser()
            var first = ByteBuffer(bytes: header.prefix(splitIndex))
            XCTAssertEqual(try parser.parse(&first), .pending, "\(vector.id) split \(splitIndex)")
            XCTAssertEqual(first.readableBytes, 0, "\(vector.id) split \(splitIndex)")
            var second = ByteBuffer(bytes: header.dropFirst(splitIndex) + application)
            guard case .complete(let metadata) = try parser.parse(&second) else {
                return XCTFail("expected completion for \(vector.id) split \(splitIndex)")
            }
            assert(metadata, source: source, destination: destination, tlvCount: vector.tlvCount, id: vector.id)
            XCTAssertTrue(second.readableBytesView.elementsEqual(application), vector.id)
        }

        var parser = ProxyV2Parser()
        for byte in header.dropLast() {
            var input = ByteBuffer(bytes: [byte])
            XCTAssertEqual(try parser.parse(&input), .pending, vector.id)
        }
        var final = ByteBuffer(bytes: [header[header.count - 1]] + application)
        guard case .complete(let metadata) = try parser.parse(&final) else {
            return XCTFail("expected one-byte completion for \(vector.id)")
        }
        assert(metadata, source: source, destination: destination, tlvCount: vector.tlvCount, id: vector.id)
        XCTAssertTrue(final.readableBytesView.elementsEqual(application), vector.id)
    }

    private func assert(
        _ metadata: ProxyV2Metadata,
        source: Phase5ProxyV2Endpoint,
        destination: Phase5ProxyV2Endpoint,
        tlvCount: Int?,
        id: String
    ) {
        XCTAssertEqual(metadata.sourceAddress.ipAddress, source.address, id)
        XCTAssertEqual(metadata.sourceAddress.port, source.port, id)
        XCTAssertEqual(metadata.destinationAddress.ipAddress, destination.address, id)
        XCTAssertEqual(metadata.destinationAddress.port, destination.port, id)
        XCTAssertEqual(metadata.tlvCount, tlvCount, id)
    }

    private func reason(for error: ProxyV2ParserError?) -> String? {
        guard let error else { return nil }
        return switch error {
        case .invalidSignature, .unsupportedVersion, .unsupportedCommand, .unsupportedAddressFamily,
             .unsupportedTransport, .headerTooLarge:
            headerReason(for: error)
        case .malformedAddressBlock, .invalidPort, .malformedTLV, .truncatedHeader, .parserAlreadyCompleted:
            payloadReason(for: error)
        }
    }

    private func headerReason(for error: ProxyV2ParserError) -> String {
        switch error {
        case .invalidSignature:
            "invalid-signature"
        case .unsupportedVersion:
            "unsupported-version"
        case .unsupportedCommand:
            "unsupported-command"
        case .unsupportedAddressFamily:
            "unsupported-address-family"
        case .unsupportedTransport:
            "unsupported-transport"
        case .headerTooLarge:
            "header-too-large"
        default:
            preconditionFailure("not a header policy error")
        }
    }

    private func payloadReason(for error: ProxyV2ParserError) -> String {
        switch error {
        case .malformedAddressBlock:
            "malformed-address-block"
        case .invalidPort:
            "invalid-port"
        case .malformedTLV:
            "malformed-tlv"
        case .truncatedHeader:
            "truncated-header"
        case .parserAlreadyCompleted:
            "parser-already-completed"
        default:
            preconditionFailure("not a payload policy error")
        }
    }
}

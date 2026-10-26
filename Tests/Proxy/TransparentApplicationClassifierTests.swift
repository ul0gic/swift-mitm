import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class TransparentApplicationClassifierTests: XCTestCase {
    func testFragmentedTLSClassificationPreservesMetadataAndReplaysEveryByteOnce() throws {
        let bytes = makeTLSClientHello(
            protocols: [Array("h2".utf8), Array("http/1.1".utf8)],
            serverName: "API.Example.COM"
        )
        let storage = DecisionStorage()
        let channel = try makeChannel(storage: storage)
        let first = Array(bytes.prefix(7))
        let second = Array(bytes.dropFirst(7))

        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: first)))
        XCTAssertNil(storage.classification)
        assertFull(try channel.writeInbound(ByteBuffer(bytes: second)))
        channel.embeddedEventLoop.run()

        guard case .interceptedTLS(let metadata) = storage.classification else {
            return XCTFail("expected intercepted TLS")
        }
        XCTAssertEqual(metadata.serverName, "api.example.com")
        XCTAssertEqual(metadata.supportedALPNProtocols, [.http2, .http11])
        XCTAssertEqual(storage.decisionCount, 1)
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), first)
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), second)
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
        XCTAssertNoThrow(try channel.finish())
    }

    func testTLSWithoutALPNUsesHTTP1InterceptionCompatibility() throws {
        let storage = DecisionStorage()
        let channel = try makeChannel(storage: storage)

        assertFull(try channel.writeInbound(ByteBuffer(bytes: makeTLSClientHello(protocols: nil))))
        channel.embeddedEventLoop.run()

        guard case .interceptedTLS(let metadata) = storage.classification else {
            return XCTFail("expected intercepted TLS")
        }
        XCTAssertFalse(metadata.hasALPNExtension)
        XCTAssertEqual(metadata.supportedALPNProtocols, [])
        XCTAssertNoThrow(try channel.finish())
    }

    func testEveryTLSNetworkFragmentBoundaryReplaysExactlyOnce() throws {
        let bytes = makeTLSClientHello(protocols: [Array("h2".utf8)])

        for boundary in 1 ..< bytes.count {
            let storage = DecisionStorage()
            let channel = try makeChannel(storage: storage)
            let first = Array(bytes.prefix(boundary))
            let second = Array(bytes.dropFirst(boundary))

            assertEmpty(try channel.writeInbound(ByteBuffer(bytes: first)))
            assertFull(try channel.writeInbound(ByteBuffer(bytes: second)))
            channel.embeddedEventLoop.run()

            guard case .interceptedTLS = storage.classification else {
                return XCTFail("expected intercepted TLS at boundary \(boundary)")
            }
            XCTAssertEqual(
                Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView),
                first
            )
            XCTAssertEqual(
                Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView),
                second
            )
            XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
            XCTAssertNoThrow(try channel.finish())
        }
    }

    func testECHClientHelloFallsBackToOpaque() throws {
        let storage = DecisionStorage()
        let channel = try makeChannel(storage: storage)
        let bytes = makeTLSClientHello(protocols: [Array("h2".utf8)], ech: true)

        assertFull(try channel.writeInbound(ByteBuffer(bytes: bytes)))
        channel.embeddedEventLoop.run()

        guard case .opaque = storage.classification else {
            return XCTFail("expected opaque classification")
        }
        XCTAssertNoThrow(try channel.finish())
    }

    func testUnsupportedOpaqueALPNIdentifiersFallBackToOpaqueLosslessly() throws {
        let storage = DecisionStorage()
        let channel = try makeChannel(storage: storage)
        let bytes = makeTLSClientHello(protocols: [[0xFF, 0x00], [0x80]])

        assertFull(try channel.writeInbound(ByteBuffer(bytes: bytes)))
        channel.embeddedEventLoop.run()

        guard case .opaque = storage.classification else {
            return XCTFail("expected opaque classification")
        }
        XCTAssertEqual(
            Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView),
            bytes
        )
        XCTAssertNoThrow(try channel.finish())
    }

    func testMixedOpaqueAndSupportedALPNIdentifiersRemainIntercepted() throws {
        let storage = DecisionStorage()
        let channel = try makeChannel(storage: storage)
        let protocols: [[UInt8]] = [[0xFF, 0x00], Array("h2".utf8), [0x80]]

        assertFull(try channel.writeInbound(ByteBuffer(bytes: makeTLSClientHello(protocols: protocols))))
        channel.embeddedEventLoop.run()

        guard case .interceptedTLS(let metadata) = storage.classification else {
            return XCTFail("expected intercepted TLS")
        }
        XCTAssertEqual(metadata.offeredALPNProtocols.map(\.bytes), protocols)
        XCTAssertEqual(metadata.supportedALPNProtocols, [.http2])
        XCTAssertNoThrow(try channel.finish())
    }

    func testClearHTTP1RequestLineIsRecognizedWithoutConsumingOrReordering() throws {
        let storage = DecisionStorage()
        let channel = try makeChannel(storage: storage)
        let first = Array("PATCH /v1/items?x=1 HT".utf8)
        let second = Array("TP/1.1\r\nHost: example.com\r\n\r\nbody".utf8)

        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: first)))
        XCTAssertNil(storage.classification)
        assertFull(try channel.writeInbound(ByteBuffer(bytes: second)))
        channel.embeddedEventLoop.run()

        guard case .clearHTTP1 = storage.classification else {
            return XCTFail("expected clear HTTP/1 classification")
        }
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), first)
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), second)
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
        XCTAssertNoThrow(try channel.finish())
    }

    func testHTTP2PriorKnowledgeAndUnknownBinaryPrefixesAreOpaque() throws {
        for bytes in [Array("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".utf8), [0x01, 0x02, 0x03]] {
            let storage = DecisionStorage()
            let channel = try makeChannel(storage: storage)

            assertFull(try channel.writeInbound(ByteBuffer(bytes: bytes)))
            channel.embeddedEventLoop.run()

            guard case .opaque = storage.classification else {
                return XCTFail("expected opaque classification")
            }
            XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), bytes)
            XCTAssertNoThrow(try channel.finish())
        }
    }

    func testDeadlineClassifiesZeroUnknownAndIncompleteTLSPrefixesAsOpaque() throws {
        for bytes in [[], Array("SSH-2".utf8), [22, 3]] {
            let storage = DecisionStorage()
            let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
                trustedPeers: .loopback,
                classificationDeadline: .milliseconds(10)
            ))
            let channel = try makeChannel(configuration: configuration, storage: storage)
            if !bytes.isEmpty {
                assertEmpty(try channel.writeInbound(ByteBuffer(bytes: bytes)))
            }

            channel.embeddedEventLoop.advanceTime(by: .milliseconds(10))
            channel.embeddedEventLoop.run()

            guard case .opaque = storage.classification else {
                return XCTFail("expected opaque classification")
            }
            if !bytes.isEmpty {
                XCTAssertEqual(
                    Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView),
                    bytes
                )
            }
            XCTAssertNoThrow(try channel.finish())
        }
    }

    func testUnknownTextAtClassificationBoundFallsBackToOpaque() throws {
        let storage = DecisionStorage()
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            classificationMaximumBytes: 4
        ))
        let channel = try makeChannel(configuration: configuration, storage: storage)
        let bytes = Array("ABCD".utf8)

        assertFull(try channel.writeInbound(ByteBuffer(bytes: bytes)))
        channel.embeddedEventLoop.run()

        guard case .opaque = storage.classification else {
            return XCTFail("expected opaque classification")
        }
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), bytes)
        XCTAssertNoThrow(try channel.finish())
    }

    func testMalformedTLSClientHelloFailsTypedAndClosesWithoutReplay() throws {
        let storage = DecisionStorage()
        let errors = ClassifierErrorStorage()
        let channel = try makeChannel(storage: storage, errors: errors)
        let malformed: [UInt8] = [22, 3, 3, 0, 4, 2, 0, 0, 0]

        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: malformed)))
        channel.embeddedEventLoop.run()

        XCTAssertFalse(channel.isActive)
        XCTAssertEqual(errors.classificationErrors, [.malformedTLSClientHello])
        XCTAssertNil(storage.classification)
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testIncompleteTLSClientHelloBeyondConfiguredLimitFailsTyped() throws {
        let storage = DecisionStorage()
        let errors = ClassifierErrorStorage()
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            classificationMaximumBytes: 8
        ))
        let channel = try makeChannel(configuration: configuration, storage: storage, errors: errors)

        assertEmpty(try channel.writeInbound(ByteBuffer(bytes: [22, 3, 3, 0, 32, 1, 0, 0, 0])))
        channel.embeddedEventLoop.run()

        XCTAssertFalse(channel.isActive)
        XCTAssertEqual(errors.classificationErrors, [.tlsClientHelloExceedsClassificationLimit])
        XCTAssertNil(storage.classification)
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testCoalescedPostClientHelloBytesDoNotCountAgainstClassificationLimit() throws {
        let hello = makeTLSClientHello(protocols: [Array("h2".utf8)])
        let applicationBytes = Array(repeating: UInt8(0xA5), count: 128)
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            classificationMaximumBytes: hello.count
        ))
        let storage = DecisionStorage()
        let channel = try makeChannel(configuration: configuration, storage: storage)

        assertFull(try channel.writeInbound(ByteBuffer(bytes: hello + applicationBytes)))
        channel.embeddedEventLoop.run()

        guard case .interceptedTLS = storage.classification else {
            return XCTFail("expected intercepted TLS")
        }
        XCTAssertEqual(
            Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView),
            hello + applicationBytes
        )
        XCTAssertNoThrow(try channel.finish())
    }

    func testDecisionOccursOnceAndSubsequentReadsPassThrough() throws {
        let storage = DecisionStorage()
        let channel = try makeChannel(storage: storage)

        assertFull(try channel.writeInbound(ByteBuffer(bytes: [0x01])))
        channel.embeddedEventLoop.run()
        let producedInbound = try channel.writeInbound(ByteBuffer(bytes: [0x02]))
        guard case .full = producedInbound else {
            return XCTFail("expected readable inbound bytes")
        }

        XCTAssertEqual(storage.decisionCount, 1)
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), [0x01])
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), [0x02])
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
        XCTAssertNoThrow(try channel.finish())
    }

    func testManualReadModeLeavesAutoReadDisabledAndIssuesOneReadAfterReplay() throws {
        let storage = DecisionStorage()
        let readCounter = ClassifierReadCounter()
        let channel = try makeChannel(storage: storage, readMode: .manual, readCounter: readCounter)

        assertFull(try channel.writeInbound(ByteBuffer(bytes: [0x01])))
        channel.embeddedEventLoop.run()

        XCTAssertEqual(readCounter.count, 1)
        XCTAssertEqual(Array(try XCTUnwrap(channel.readInbound(as: ByteBuffer.self)).readableBytesView), [0x01])
        XCTAssertNoThrow(try channel.finish())
    }
}

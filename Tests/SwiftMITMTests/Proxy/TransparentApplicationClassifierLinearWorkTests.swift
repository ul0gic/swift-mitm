import NIOCore
import XCTest

@testable import SwiftMITM

extension TransparentApplicationClassifierTests {
    func testOneByteTLSClassificationHasFragmentationIndependentLinearWork() throws {
        let bytes = makeTLSClientHello(
            protocols: [Array("h2".utf8), Array("http/1.1".utf8)],
            serverName: "API.Example.COM"
        )
        let fragmentedStorage = DecisionStorage()
        let fragmented = try makeChannelWithClassifier(storage: fragmentedStorage)

        for (index, byte) in bytes.enumerated() {
            let state = try fragmented.channel.writeInbound(ByteBuffer(bytes: CollectionOfOne(byte)))
            if index < bytes.count - 1 {
                assertEmpty(state)
            } else {
                assertFull(state)
            }
        }
        fragmented.channel.embeddedEventLoop.run()

        let contiguousStorage = DecisionStorage()
        let contiguous = try makeChannelWithClassifier(storage: contiguousStorage)
        assertFull(try contiguous.channel.writeInbound(ByteBuffer(bytes: bytes)))
        contiguous.channel.embeddedEventLoop.run()

        guard case .interceptedTLS(let metadata) = fragmentedStorage.classification else {
            return XCTFail("expected intercepted TLS")
        }
        XCTAssertEqual(metadata.serverName, "api.example.com")
        XCTAssertEqual(metadata.supportedALPNProtocols, [.http2, .http11])
        XCTAssertEqual(fragmented.classifier.classificationWork, contiguous.classifier.classificationWork)
        XCTAssertEqual(fragmented.classifier.classificationWork.tls.copiedByteCount, bytes.count)
        XCTAssertEqual(fragmented.classifier.classificationWork.tls.completeInspectionCount, 1)
        XCTAssertLessThanOrEqual(
            fragmented.classifier.classificationWork.tls.totalByteCount,
            bytes.count * 3
        )
        XCTAssertEqual(try readAllInboundBytes(fragmented.channel), bytes)
        XCTAssertNoThrow(try fragmented.channel.finish())
        XCTAssertNoThrow(try contiguous.channel.finish())
    }

    func testOneByteIncompleteTLSAtClassificationLimitHasLinearWorkAndFailsTyped() throws {
        let maximumBytes = 64
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            classificationMaximumBytes: maximumBytes
        ))
        let storage = DecisionStorage()
        let errors = ClassifierErrorStorage()
        let fixture = try makeChannelWithClassifier(
            configuration: configuration,
            storage: storage,
            errors: errors
        )
        let bytes = [UInt8(22), 3, 3, 0xFF, 0xFF] + Array(repeatElement(0, count: maximumBytes - 5))

        for byte in bytes {
            assertEmpty(try fixture.channel.writeInbound(ByteBuffer(bytes: CollectionOfOne(byte))))
        }
        fixture.channel.embeddedEventLoop.run()

        XCTAssertFalse(fixture.channel.isActive)
        XCTAssertEqual(errors.classificationErrors, [.tlsClientHelloExceedsClassificationLimit])
        XCTAssertEqual(fixture.classifier.classificationWork.tls.copiedByteCount, maximumBytes)
        XCTAssertEqual(fixture.classifier.classificationWork.tls.completeInspectionCount, 0)
        XCTAssertLessThanOrEqual(
            fixture.classifier.classificationWork.tls.totalByteCount,
            maximumBytes * 2
        )
        XCTAssertNoThrow(try fixture.channel.finish(acceptAlreadyClosed: true))
    }

    func testOneByteMalformedTLSHasLinearWorkAndFailsTyped() throws {
        let storage = DecisionStorage()
        let errors = ClassifierErrorStorage()
        let fixture = try makeChannelWithClassifier(storage: storage, errors: errors)
        let bytes: [UInt8] = [22, 3, 3, 0, 4, 2, 0, 0, 0]

        for byte in bytes {
            assertEmpty(try fixture.channel.writeInbound(ByteBuffer(bytes: CollectionOfOne(byte))))
        }
        fixture.channel.embeddedEventLoop.run()

        XCTAssertFalse(fixture.channel.isActive)
        XCTAssertEqual(errors.classificationErrors, [.malformedTLSClientHello])
        XCTAssertEqual(fixture.classifier.classificationWork.tls.copiedByteCount, bytes.count)
        XCTAssertEqual(fixture.classifier.classificationWork.tls.completeInspectionCount, 0)
        XCTAssertLessThanOrEqual(fixture.classifier.classificationWork.tls.totalByteCount, bytes.count * 2)
        XCTAssertNoThrow(try fixture.channel.finish(acceptAlreadyClosed: true))
    }

    func testOneByteHTTP1ClassificationProcessesEachPrefixByteOnce() throws {
        let bytes = Array("PATCH /v1/items?x=1 HTTP/1.1\r\n".utf8)
        let fragmentedStorage = DecisionStorage()
        let fragmented = try makeChannelWithClassifier(storage: fragmentedStorage)

        for (index, byte) in bytes.enumerated() {
            let state = try fragmented.channel.writeInbound(ByteBuffer(bytes: CollectionOfOne(byte)))
            if index < bytes.count - 1 {
                assertEmpty(state)
            } else {
                assertFull(state)
            }
        }
        fragmented.channel.embeddedEventLoop.run()

        let contiguousStorage = DecisionStorage()
        let contiguous = try makeChannelWithClassifier(storage: contiguousStorage)
        assertFull(try contiguous.channel.writeInbound(ByteBuffer(bytes: bytes)))
        contiguous.channel.embeddedEventLoop.run()

        guard case .clearHTTP1 = fragmentedStorage.classification else {
            return XCTFail("expected clear HTTP/1 classification")
        }
        XCTAssertEqual(fragmented.classifier.classificationWork, contiguous.classifier.classificationWork)
        XCTAssertEqual(fragmented.classifier.classificationWork.httpPrefixByteCount, bytes.count)
        XCTAssertEqual(try readAllInboundBytes(fragmented.channel), bytes)
        XCTAssertNoThrow(try fragmented.channel.finish())
        XCTAssertNoThrow(try contiguous.channel.finish())
    }

    func testOneByteOpaqueClassificationProcessesNoByteMoreThanOnce() throws {
        let immediateStorage = DecisionStorage()
        let immediate = try makeChannelWithClassifier(storage: immediateStorage)
        assertFull(try immediate.channel.writeInbound(ByteBuffer(bytes: [0x01])))
        immediate.channel.embeddedEventLoop.run()

        let boundedStorage = DecisionStorage()
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: .loopback,
            classificationMaximumBytes: 4
        ))
        let bounded = try makeChannelWithClassifier(configuration: configuration, storage: boundedStorage)
        let bytes = Array("ABCD".utf8)
        for (index, byte) in bytes.enumerated() {
            let state = try bounded.channel.writeInbound(ByteBuffer(bytes: CollectionOfOne(byte)))
            if index < bytes.count - 1 {
                assertEmpty(state)
            } else {
                assertFull(state)
            }
        }
        bounded.channel.embeddedEventLoop.run()

        guard case .opaque = immediateStorage.classification else {
            return XCTFail("expected immediate opaque classification")
        }
        guard case .opaque = boundedStorage.classification else {
            return XCTFail("expected bounded opaque classification")
        }
        XCTAssertEqual(immediate.classifier.classificationWork.httpPrefixByteCount, 1)
        XCTAssertEqual(bounded.classifier.classificationWork.httpPrefixByteCount, bytes.count)
        XCTAssertEqual(try readAllInboundBytes(bounded.channel), bytes)
        XCTAssertNoThrow(try immediate.channel.finish())
        XCTAssertNoThrow(try bounded.channel.finish())
    }
}

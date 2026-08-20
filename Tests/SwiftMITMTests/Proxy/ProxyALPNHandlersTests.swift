import NIOCore
import NIOConcurrencyHelpers
import NIOEmbedded
import NIOTLS
import XCTest

@testable import SwiftMITM

final class ProxyALPNHandlersTests: XCTestCase {
    func testIncrementalInspectorOneByteFragmentsHaveFragmentationIndependentLinearWork() throws {
        let bytes = makeClientHelloRecords(protocols: ["h2", "http/1.1"], payloadLengths: [2, 11])
        var fragmentedInspector = IncrementalClientHelloInspector()
        var fragmentedMetadata: ClientHelloMetadata?

        for (index, byte) in bytes.enumerated() {
            fragmentedMetadata = try fragmentedInspector.append(CollectionOfOne(byte))
            if index < bytes.count - 1 {
                XCTAssertNil(fragmentedMetadata)
            }
        }

        var contiguousInspector = IncrementalClientHelloInspector()
        let contiguousMetadata = try contiguousInspector.append(bytes)
        XCTAssertEqual(fragmentedMetadata?.supportedALPNProtocols, [.http2, .http11])
        XCTAssertEqual(contiguousMetadata?.supportedALPNProtocols, [.http2, .http11])
        XCTAssertEqual(fragmentedInspector.work, contiguousInspector.work)
        XCTAssertEqual(fragmentedInspector.work.copiedByteCount, bytes.count)
        XCTAssertEqual(fragmentedInspector.work.completeInspectionByteCount, bytes.count)
        XCTAssertEqual(fragmentedInspector.work.completeInspectionCount, 1)
        XCTAssertLessThanOrEqual(fragmentedInspector.work.totalByteCount, bytes.count * 3)
    }

    func testIncrementalInspectorOneByteIncompleteClientHelloNeverRunsCompleteInspection() throws {
        let bytes = makeClientHelloRecords(protocols: ["h2"]).dropLast()
        var inspector = IncrementalClientHelloInspector()

        for byte in bytes {
            XCTAssertNil(try inspector.append(CollectionOfOne(byte)))
        }

        XCTAssertEqual(inspector.work.copiedByteCount, bytes.count)
        XCTAssertEqual(inspector.work.completeInspectionByteCount, 0)
        XCTAssertEqual(inspector.work.completeInspectionCount, 0)
        XCTAssertLessThanOrEqual(inspector.work.totalByteCount, bytes.count * 2)
    }

    func testIncrementalInspectorOneByteMalformedClientHelloFailsWithoutCompleteInspection() {
        let bytes: [UInt8] = [22, 3, 3, 0, 4, 2, 0, 0, 0]
        var inspector = IncrementalClientHelloInspector()
        var observedError: Error?

        for byte in bytes {
            do {
                _ = try inspector.append(CollectionOfOne(byte))
            } catch {
                observedError = error
                break
            }
        }

        XCTAssertNotNil(observedError)
        XCTAssertEqual(inspector.work.copiedByteCount, bytes.count)
        XCTAssertEqual(inspector.work.completeInspectionByteCount, 0)
        XCTAssertEqual(inspector.work.completeInspectionCount, 0)
        XCTAssertLessThanOrEqual(inspector.work.totalByteCount, bytes.count * 2)
    }

    func testIncrementalInspectorOneByteOversizedClientHelloFailsAtAbsoluteLimit() throws {
        let maximumBytes = ClientHelloALPNParser.maximumClientHelloBytes
        var inspector = IncrementalClientHelloInspector()
        let prefix: [UInt8] = [22, 3, 3, 0xFF, 0xFF]

        for byte in prefix {
            XCTAssertNil(try inspector.append(CollectionOfOne(byte)))
        }
        for _ in prefix.count ..< maximumBytes {
            XCTAssertNil(try inspector.append(CollectionOfOne(0)))
        }

        XCTAssertThrowsError(try inspector.append(CollectionOfOne(0)))
        XCTAssertEqual(inspector.work.copiedByteCount, maximumBytes)
        XCTAssertEqual(inspector.work.completeInspectionByteCount, 0)
        XCTAssertEqual(inspector.work.completeInspectionCount, 0)
        XCTAssertEqual(inspector.work.totalByteCount, maximumBytes + 5)
    }

    func testPendingTLSReadsResolvesFragmentedClientHelloOnceComplete() throws {
        let channel = EmbeddedChannel()
        let handler = PendingTLSReads(eventLoop: channel.eventLoop)
        try channel.pipeline.syncOperations.addHandler(handler)
        let bytes = makeClientHelloRecords(protocols: ["h2", "http/1.1"], payloadLengths: [2, 11])
        let completed = NIOLockedValueBox(false)
        handler.clientALPNOffer.whenComplete { _ in completed.withLockedValue { $0 = true } }

        for (index, byte) in bytes.enumerated() {
            try channel.writeInbound(channel.allocator.buffer(bytes: CollectionOfOne(byte)))
            if index < bytes.count - 1 {
                XCTAssertFalse(completed.withLockedValue { $0 })
            }
        }

        guard case .protocols(let protocols) = try handler.clientALPNOffer.wait() else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertEqual(protocols, [.http2, .http11])
        XCTAssertEqual(handler.inspectionWork.copiedByteCount, bytes.count)
        XCTAssertEqual(handler.inspectionWork.completeInspectionCount, 1)
        XCTAssertNoThrow(try channel.finish())
    }

    func testPendingTLSReadsRejectsMoreThan64KiB() throws {
        let channel = EmbeddedChannel()
        let handler = PendingTLSReads(eventLoop: channel.eventLoop)
        try channel.pipeline.syncOperations.addHandler(handler)
        let closed = NIOLockedValueBox(false)
        channel.closeFuture.whenComplete { _ in closed.withLockedValue { $0 = true } }
        var pendingRecord = [UInt8(22), 3, 3, 0xFF, 0xFF]
        pendingRecord.append(contentsOf: repeatElement(0, count: 65_531))

        try channel.writeInbound(channel.allocator.buffer(bytes: pendingRecord))
        XCTAssertFalse(closed.withLockedValue { $0 })
        try channel.writeInbound(channel.allocator.buffer(bytes: [0]))
        channel.embeddedEventLoop.run()

        XCTAssertTrue(closed.withLockedValue { $0 })
        XCTAssertThrowsError(try handler.clientALPNOffer.wait())
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testUpstreamHandlerUsesHTTP1FallbackAndCompletesPromiseOnce() throws {
        let channel = EmbeddedChannel()
        let negotiated = channel.eventLoop.makePromise(of: ALPNProtocol.self)
        let handler = UpstreamALPNHandler(negotiatedProtocol: negotiated)
        try channel.pipeline.syncOperations.addHandler(handler)

        channel.pipeline.fireUserInboundEventTriggered(TLSUserEvent.handshakeCompleted(negotiatedProtocol: nil))
        channel.embeddedEventLoop.run()
        channel.pipeline.fireUserInboundEventTriggered(TLSUserEvent.handshakeCompleted(negotiatedProtocol: "h2"))
        channel.embeddedEventLoop.run()

        XCTAssertEqual(try negotiated.futureResult.wait(), .http11)
        XCTAssertNoThrow(try channel.finish())
    }

    func testUpstreamHandlerRejectsUnsupportedNegotiatedProtocol() throws {
        let channel = EmbeddedChannel()
        let negotiated = channel.eventLoop.makePromise(of: ALPNProtocol.self)
        try channel.pipeline.syncOperations.addHandler(UpstreamALPNHandler(negotiatedProtocol: negotiated))

        channel.pipeline.fireUserInboundEventTriggered(
            TLSUserEvent.handshakeCompleted(negotiatedProtocol: "unsupported")
        )
        channel.embeddedEventLoop.run()

        XCTAssertThrowsError(try negotiated.futureResult.wait())
        XCTAssertNoThrow(try channel.finish())
    }

    func testUpstreamHandlerFailsPromiseWhenChannelClosesBeforeHandshake() throws {
        let channel = EmbeddedChannel()
        let negotiated = channel.eventLoop.makePromise(of: ALPNProtocol.self)
        try channel.pipeline.syncOperations.addHandler(UpstreamALPNHandler(negotiatedProtocol: negotiated))

        try channel.close().wait()

        XCTAssertThrowsError(try negotiated.futureResult.wait())
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    func testUpstreamHandlerBuffersAndReplaysPlaintextAfterPipelineIsReady() throws {
        let channel = EmbeddedChannel()
        let negotiated = channel.eventLoop.makePromise(of: ALPNProtocol.self)
        let handler = UpstreamALPNHandler(negotiatedProtocol: negotiated)
        try channel.pipeline.syncOperations.addHandler(handler)
        channel.pipeline.fireUserInboundEventTriggered(TLSUserEvent.handshakeCompleted(negotiatedProtocol: "h2"))
        channel.embeddedEventLoop.run()
        XCTAssertEqual(try negotiated.futureResult.wait(), .http2)

        try channel.writeInbound(channel.allocator.buffer(bytes: [1, 2]))
        try channel.writeInbound(channel.allocator.buffer(bytes: [3, 4]))
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))

        try handler.releaseBufferedReads().wait()

        XCTAssertEqual(try channel.readInbound(as: ByteBuffer.self)?.readableBytesView.elementsEqual([1, 2]), true)
        XCTAssertEqual(try channel.readInbound(as: ByteBuffer.self)?.readableBytesView.elementsEqual([3, 4]), true)
        XCTAssertNoThrow(try channel.finish())
    }

    func testUpstreamHandlerClosesWhenPlaintextBufferExceeds64KiB() throws {
        let channel = EmbeddedChannel()
        let negotiated = channel.eventLoop.makePromise(of: ALPNProtocol.self)
        let handler = UpstreamALPNHandler(negotiatedProtocol: negotiated)
        try channel.pipeline.syncOperations.addHandler(handler)
        let closed = NIOLockedValueBox(false)
        channel.closeFuture.whenComplete { _ in closed.withLockedValue { $0 = true } }
        channel.pipeline.fireUserInboundEventTriggered(TLSUserEvent.handshakeCompleted(negotiatedProtocol: "h2"))
        channel.embeddedEventLoop.run()
        XCTAssertEqual(try negotiated.futureResult.wait(), .http2)

        try channel.writeInbound(channel.allocator.buffer(bytes: repeatElement(0, count: 65_536)))
        XCTAssertFalse(closed.withLockedValue { $0 })
        try channel.writeInbound(channel.allocator.buffer(bytes: [0]))
        channel.embeddedEventLoop.run()

        XCTAssertTrue(closed.withLockedValue { $0 })
        XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
    }

    private func makeClientHelloRecords(protocols: [String], payloadLengths: [Int] = []) -> [UInt8] {
        let names = protocols.flatMap { name -> [UInt8] in
            let bytes = Array(name.utf8)
            return [UInt8(bytes.count)] + bytes
        }
        let alpn = encodedUInt16(names.count) + names
        let extensions = [UInt8(0), 16] + encodedUInt16(alpn.count) + alpn
        var body: [UInt8] = [3, 3]
        body.append(contentsOf: repeatElement(0, count: 32))
        body.append(0)
        body.append(contentsOf: [0, 2, 0x13, 0x01])
        body.append(contentsOf: [1, 0])
        body.append(contentsOf: encodedUInt16(extensions.count))
        body.append(contentsOf: extensions)
        let handshake = [UInt8(1)] + encodedUInt24(body.count) + body
        var records: [UInt8] = []
        var index = 0
        for requestedLength in payloadLengths where index < handshake.count {
            let length = min(requestedLength, handshake.count - index)
            records.append(contentsOf: [22, 3, 3] + encodedUInt16(length))
            records.append(contentsOf: handshake[index ..< index + length])
            index += length
        }
        if index < handshake.count {
            let length = handshake.count - index
            records.append(contentsOf: [22, 3, 3] + encodedUInt16(length))
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

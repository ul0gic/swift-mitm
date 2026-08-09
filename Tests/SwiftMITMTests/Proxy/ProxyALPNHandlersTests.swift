import NIOCore
import NIOConcurrencyHelpers
import NIOEmbedded
import NIOTLS
import XCTest

@testable import SwiftMITM

final class ProxyALPNHandlersTests: XCTestCase {
    func testPendingTLSReadsResolvesFragmentedClientHelloOnceComplete() throws {
        let channel = EmbeddedChannel()
        let handler = PendingTLSReads(eventLoop: channel.eventLoop)
        try channel.pipeline.syncOperations.addHandler(handler)
        let bytes = makeClientHelloRecord(protocols: ["h2", "http/1.1"])
        let completed = NIOLockedValueBox(false)
        handler.clientALPNOffer.whenComplete { _ in completed.withLockedValue { $0 = true } }

        try channel.writeInbound(channel.allocator.buffer(bytes: bytes.prefix(7)))
        XCTAssertFalse(completed.withLockedValue { $0 })
        try channel.writeInbound(channel.allocator.buffer(bytes: bytes.dropFirst(7)))

        guard case .protocols(let protocols) = try handler.clientALPNOffer.wait() else {
            return XCTFail("expected an ALPN offer")
        }
        XCTAssertEqual(protocols, [.http2, .http11])
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

    private func makeClientHelloRecord(protocols: [String]) -> [UInt8] {
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
        return [22, 3, 3] + encodedUInt16(handshake.count) + handshake
    }

    private func encodedUInt16(_ value: Int) -> [UInt8] {
        [UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }

    private func encodedUInt24(_ value: Int) -> [UInt8] {
        [UInt8((value >> 16) & 0xFF), UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }
}

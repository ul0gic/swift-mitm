import NIOCore
import NIOTLS

struct NegotiatedUpstreamConnection: Sendable {
    let channel: Channel
    let applicationProtocol: ALPNProtocol
    let alpnHandler: NIOLoopBound<UpstreamALPNHandler>

    func releaseBufferedReads() -> EventLoopFuture<Void> {
        alpnHandler.value.releaseBufferedReads()
    }
}

enum ProxyALPNError: Error {
    case channelClosedBeforeNegotiation
    case malformedClientHello
    case noSupportedClientProtocol
    case protocolMismatch(downstream: String, upstream: String)
    case unsupportedProtocol(String)
}

enum ClientALPNOffer: Sendable {
    case absent
    case protocols([ALPNProtocol])
}

final class UpstreamALPNHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private static let maximumBufferedBytes = 65_536

    private let negotiatedProtocol: EventLoopPromise<ALPNProtocol>
    private var context: ChannelHandlerContext?
    private var reads: [ByteBuffer] = []
    private var bufferedByteCount = 0
    private var readComplete = false
    private var completed = false
    private var released = false

    init(negotiatedProtocol: EventLoopPromise<ALPNProtocol>) {
        self.negotiatedProtocol = negotiatedProtocol
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.context = nil
        reads.removeAll(keepingCapacity: false)
        bufferedByteCount = 0
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !released else {
            context.fireChannelRead(data)
            return
        }
        let buffer = unwrapInboundIn(data)
        bufferedByteCount += buffer.readableBytes
        guard bufferedByteCount <= Self.maximumBufferedBytes else {
            fail(ProxyALPNError.channelClosedBeforeNegotiation, context: context)
            context.close(promise: nil)
            return
        }
        reads.append(buffer)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        guard !released else {
            context.fireChannelReadComplete()
            return
        }
        readComplete = true
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let protocolName) = event {
            let result: Result<ALPNProtocol, Error>
            if let protocolName {
                result = ALPNProtocol(rawValue: protocolName)
                    .map(Result.success)
                    ?? .failure(ProxyALPNError.unsupportedProtocol(protocolName))
            } else {
                result = .success(.http11)
            }
            switch result {
            case .success(let negotiated):
                guard !completed else { break }
                completed = true
                context.channel.setOption(ChannelOptions.autoRead, value: false)
                    .map { negotiated }
                    .cascade(to: negotiatedProtocol)
            case .failure(let error):
                fail(error, context: context)
            }
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        fail(ProxyALPNError.channelClosedBeforeNegotiation, context: context)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error, context: context)
        context.fireErrorCaught(error)
    }

    func releaseBufferedReads() -> EventLoopFuture<Void> {
        guard let context else {
            return negotiatedProtocol.futureResult.eventLoop.makeFailedFuture(
                ProxyALPNError.channelClosedBeforeNegotiation
            )
        }
        released = true
        reads.forEach { context.fireChannelRead(wrapInboundOut($0)) }
        if readComplete {
            context.fireChannelReadComplete()
        }
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
        return context.channel.setOption(ChannelOptions.autoRead, value: true)
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        guard !completed else { return }
        completed = true
        negotiatedProtocol.fail(error)
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
    }
}

struct ClientHelloInspectionWork: Equatable, Sendable {
    private(set) var copiedByteCount = 0
    private(set) var framingByteCount = 0
    private(set) var completeInspectionByteCount = 0
    private(set) var completeInspectionCount = 0

    var totalByteCount: Int {
        copiedByteCount + framingByteCount + completeInspectionByteCount
    }

    mutating func recordCopy(byteCount: Int) {
        copiedByteCount += byteCount
    }

    mutating func recordFraming(byteCount: Int) {
        framingByteCount += byteCount
    }

    mutating func recordCompleteInspection(byteCount: Int) {
        completeInspectionByteCount += byteCount
        completeInspectionCount += 1
    }
}

struct IncrementalClientHelloInspector {
    private var bytes: [UInt8] = []
    private var recordIndex = 0
    private var recordTypeValidated = false
    private var pendingRecordEndIndex: Int?
    private var pendingRecordPayloadIndex = 0
    private var handshakeHeader: [UInt8] = []
    private var handshakeByteCount = 0
    private var expectedHandshakeByteCount: Int?
    private(set) var work = ClientHelloInspectionWork()

    init() {
        bytes.reserveCapacity(ClientHelloALPNParser.maximumClientHelloBytes)
        handshakeHeader.reserveCapacity(4)
    }

    mutating func append<Bytes: Collection>(_ newBytes: Bytes) throws -> ClientHelloMetadata?
        where Bytes.Element == UInt8 {
        guard newBytes.count <= ClientHelloALPNParser.maximumClientHelloBytes - bytes.count else {
            throw ProxyALPNError.malformedClientHello
        }
        bytes.append(contentsOf: newBytes)
        work.recordCopy(byteCount: newBytes.count)
        return try inspectAvailableRecords()
    }

    mutating func removeAll() {
        bytes.removeAll(keepingCapacity: false)
        handshakeHeader.removeAll(keepingCapacity: false)
    }

    private mutating func inspectAvailableRecords() throws -> ClientHelloMetadata? {
        while true {
            guard try beginRecordIfAvailable() else { return nil }
            guard let recordEndIndex = pendingRecordEndIndex, recordEndIndex <= bytes.count else {
                return nil
            }
            inspectHandshakeHeader(recordEndIndex: recordEndIndex)
            try establishExpectedHandshakeByteCount()
            if let metadata = try inspectCompleteClientHelloIfAvailable() {
                return metadata
            }
            recordIndex = recordEndIndex
            recordTypeValidated = false
            pendingRecordEndIndex = nil
        }
    }

    private mutating func beginRecordIfAvailable() throws -> Bool {
        guard pendingRecordEndIndex == nil else { return true }
        guard recordIndex < bytes.count else { return false }
        if !recordTypeValidated {
            work.recordFraming(byteCount: 1)
            guard bytes[recordIndex] == 22 else {
                throw ProxyALPNError.malformedClientHello
            }
            recordTypeValidated = true
        }
        guard recordIndex + 5 <= bytes.count else { return false }
        let recordLength = Self.integer(bytes[recordIndex + 3], bytes[recordIndex + 4])
        guard recordLength <= ClientHelloALPNParser.maximumClientHelloBytes else {
            throw ProxyALPNError.malformedClientHello
        }
        work.recordFraming(byteCount: 4)
        pendingRecordPayloadIndex = recordIndex + 5
        pendingRecordEndIndex = pendingRecordPayloadIndex + recordLength
        return true
    }

    private mutating func inspectHandshakeHeader(recordEndIndex: Int) {
        let headerByteCount = min(4 - handshakeHeader.count, recordEndIndex - pendingRecordPayloadIndex)
        if headerByteCount > 0 {
            handshakeHeader.append(
                contentsOf: bytes[pendingRecordPayloadIndex ..< pendingRecordPayloadIndex + headerByteCount]
            )
            work.recordFraming(byteCount: headerByteCount)
        }
        handshakeByteCount += recordEndIndex - pendingRecordPayloadIndex
    }

    private mutating func establishExpectedHandshakeByteCount() throws {
        guard handshakeHeader.count == 4, expectedHandshakeByteCount == nil else { return }
        guard handshakeHeader[0] == 1 else {
            throw ProxyALPNError.malformedClientHello
        }
        let handshakeLength = Self.integer(handshakeHeader[1], handshakeHeader[2], handshakeHeader[3])
        guard handshakeLength <= ClientHelloALPNParser.maximumClientHelloBytes - 4 else {
            throw ProxyALPNError.malformedClientHello
        }
        expectedHandshakeByteCount = handshakeLength + 4
    }

    private mutating func inspectCompleteClientHelloIfAvailable() throws -> ClientHelloMetadata? {
        guard let expectedHandshakeByteCount, handshakeByteCount >= expectedHandshakeByteCount else {
            return nil
        }
        work.recordCompleteInspection(byteCount: bytes.count)
        guard let metadata = try ClientHelloALPNParser.inspect(bytes) else {
            throw ProxyALPNError.malformedClientHello
        }
        return metadata
    }

    private static func integer(_ bytes: UInt8...) -> Int {
        bytes.reduce(0) { ($0 << 8) | Int($1) }
    }
}

final class PendingTLSReads: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    let clientALPNOffer: EventLoopFuture<ClientALPNOffer>

    private let clientALPNOfferPromise: EventLoopPromise<ClientALPNOffer>
    private var reads: [ByteBuffer] = []
    private var bufferedByteCount = 0
    private var readComplete = false
    private var released = false
    private var completed = false
    private var inspector = IncrementalClientHelloInspector()

    var inspectionWork: ClientHelloInspectionWork {
        inspector.work
    }

    init(eventLoop: EventLoop) {
        clientALPNOfferPromise = eventLoop.makePromise()
        clientALPNOffer = clientALPNOfferPromise.futureResult
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !released else {
            context.fireChannelRead(data)
            return
        }
        let buffer = unwrapInboundIn(data)
        guard
            buffer.readableBytes <= ClientHelloALPNParser.maximumClientHelloBytes - bufferedByteCount
        else {
            failIfPending(ProxyALPNError.malformedClientHello)
            context.close(promise: nil)
            return
        }
        bufferedByteCount += buffer.readableBytes
        reads.append(buffer)
        resolveClientALPNOffer(buffer: buffer, context: context)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        guard !released else {
            context.fireChannelReadComplete()
            return
        }
        readComplete = true
    }

    func replay(on pipeline: ChannelPipeline) {
        released = true
        reads.forEach { pipeline.fireChannelRead($0) }
        if readComplete {
            pipeline.fireChannelReadComplete()
        }
        reads.removeAll(keepingCapacity: false)
        inspector.removeAll()
        bufferedByteCount = 0
        readComplete = false
    }

    func channelInactive(context: ChannelHandlerContext) {
        failIfPending(ProxyALPNError.channelClosedBeforeNegotiation)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        failIfPending(error)
        context.fireErrorCaught(error)
    }

    private func resolveClientALPNOffer(buffer: ByteBuffer, context: ChannelHandlerContext) {
        guard !completed else { return }
        do {
            guard let metadata = try inspector.append(buffer.readableBytesView) else { return }
            completed = true
            context.channel.setOption(ChannelOptions.autoRead, value: false)
                .map { metadata.compatibilityALPNOffer }
                .cascade(to: clientALPNOfferPromise)
        } catch {
            failIfPending(error)
            context.close(promise: nil)
        }
    }

    private func failIfPending(_ error: Error) {
        guard !completed else { return }
        completed = true
        clientALPNOfferPromise.fail(error)
    }
}

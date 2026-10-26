import NIOCore

enum TransparentApplicationClassification: Sendable {
    case interceptedTLS(ClientHelloMetadata)
    case clearHTTP1
    case opaque
}

enum TransparentApplicationClassificationError: Error, Equatable, Sendable {
    case malformedTLSClientHello
    case tlsClientHelloExceedsClassificationLimit
}

enum TransparentApplicationReadMode: Sendable {
    case automatic
    case manual
}

struct TransparentApplicationClassificationWork: Equatable, Sendable {
    let inspectedByteCount: Int
    let httpPrefixByteCount: Int
    let tls: ClientHelloInspectionWork
}

private enum HTTP1PrefixStatus {
    case pending
    case recognized
    case notHTTP1
}

private struct IncrementalHTTP1PrefixInspector {
    private enum Phase {
        case method(hasBytes: Bool)
        case target(hasBytes: Bool)
        case version(index: Int, allowsHTTP10: Bool, allowsHTTP11: Bool)
        case terminal(HTTP1PrefixStatus)
    }

    private static let http10 = Array("HTTP/1.0\r\n".utf8)
    private static let http11 = Array("HTTP/1.1\r\n".utf8)

    private var phase = Phase.method(hasBytes: false)
    private(set) var processedByteCount = 0

    mutating func append<Bytes: Collection>(_ bytes: Bytes) -> HTTP1PrefixStatus
        where Bytes.Element == UInt8 {
        for byte in bytes {
            processedByteCount += 1
            if let status = consume(byte) {
                return status
            }
        }
        return .pending
    }

    private mutating func consume(_ byte: UInt8) -> HTTP1PrefixStatus? {
        switch phase {
        case .method(let hasBytes):
            return consumeMethodByte(byte, hasBytes: hasBytes)
        case .target(let hasBytes):
            guard !(33 ... 126).contains(byte) else {
                phase = .target(hasBytes: true)
                return nil
            }
            guard hasBytes, byte == 32 else { return finish(.notHTTP1) }
            phase = .version(index: 0, allowsHTTP10: true, allowsHTTP11: true)
            return nil
        case let .version(index, allowsHTTP10, allowsHTTP11):
            return consumeVersionByte(
                byte,
                index: index,
                allowsHTTP10: allowsHTTP10,
                allowsHTTP11: allowsHTTP11
            )
        case .terminal(let status):
            return status
        }
    }

    private mutating func consumeMethodByte(_ byte: UInt8, hasBytes: Bool) -> HTTP1PrefixStatus? {
        guard !Self.isTokenByte(byte) else {
            phase = .method(hasBytes: true)
            return nil
        }
        guard hasBytes, byte == 32 else { return finish(.notHTTP1) }
        phase = .target(hasBytes: false)
        return nil
    }

    private mutating func consumeVersionByte(
        _ byte: UInt8,
        index: Int,
        allowsHTTP10: Bool,
        allowsHTTP11: Bool
    ) -> HTTP1PrefixStatus? {
        let nextAllowsHTTP10 = allowsHTTP10 && Self.http10[index] == byte
        let nextAllowsHTTP11 = allowsHTTP11 && Self.http11[index] == byte
        guard nextAllowsHTTP10 || nextAllowsHTTP11 else { return finish(.notHTTP1) }
        let nextIndex = index + 1
        guard nextIndex < Self.http10.count else { return finish(.recognized) }
        phase = .version(
            index: nextIndex,
            allowsHTTP10: nextAllowsHTTP10,
            allowsHTTP11: nextAllowsHTTP11
        )
        return nil
    }

    private mutating func finish(_ status: HTTP1PrefixStatus) -> HTTP1PrefixStatus {
        phase = .terminal(status)
        return status
    }

    private static func isTokenByte(_ byte: UInt8) -> Bool {
        switch byte {
        case 48 ... 57, 65 ... 90, 97 ... 122, 33, 35, 36, 37, 38, 39, 42, 43, 45, 46, 94, 95, 96, 124, 126:
            return true
        default:
            return false
        }
    }
}

final class TransparentApplicationClassifier: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias DecisionHandler = (
        ChannelHandlerContext,
        TransparentApplicationClassification
    ) throws -> EventLoopFuture<TransparentApplicationReadMode>

    private enum State: Equatable {
        case classifying
        case transitioning
        case terminal
    }

    private let maximumBytes: Int
    private let deadline: TimeAmount
    private let decisionHandler: DecisionHandler
    private let stageObserver: (any TransparentIngressStageObserver)?
    private var inspectedByteCount = 0
    private var firstInspectedByte: UInt8?
    private var tlsInspector = IncrementalClientHelloInspector()
    private var http1Inspector = IncrementalHTTP1PrefixInspector()
    private var reads: [ByteBuffer] = []
    private var readComplete = false
    private var deadlineTask: Scheduled<Void>?
    private var pendingStageObserved = false
    private var state = State.classifying

    var classificationWork: TransparentApplicationClassificationWork {
        TransparentApplicationClassificationWork(
            inspectedByteCount: inspectedByteCount,
            httpPrefixByteCount: http1Inspector.processedByteCount,
            tls: tlsInspector.work
        )
    }

    init(
        configuration: TrustedProxyV2Ingress,
        stageObserver: (any TransparentIngressStageObserver)? = nil,
        decisionHandler: @escaping DecisionHandler
    ) {
        maximumBytes = configuration.classificationMaximumBytes
        deadline = configuration.classificationDeadlineTimeAmount
        self.stageObserver = stageObserver
        self.decisionHandler = decisionHandler
    }

    func handlerAdded(context: ChannelHandlerContext) {
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
        deadlineTask = context.eventLoop.scheduleTask(in: deadline) {
            guard owner.value.state == .classifying else { return }
            owner.value.beginTransition(.opaque, context: boundContext.value)
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        deadlineTask?.cancel()
        deadlineTask = nil
        state = .terminal
        tlsInspector.removeAll()
        reads.removeAll(keepingCapacity: false)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        reads.append(buffer)
        guard state == .classifying else { return }

        let remainingCapacity = maximumBytes - inspectedByteCount
        let inspectedCount = min(remainingCapacity, buffer.readableBytes)
        let inspectedBytes = buffer.readableBytesView.prefix(inspectedCount)
        inspectedByteCount += inspectedCount
        if firstInspectedByte == nil {
            firstInspectedByte = inspectedBytes.first
        }

        switch classify(inspectedBytes) {
        case .success(let classification?):
            beginTransition(classification, context: context)
        case .success(nil):
            if inspectedCount < buffer.readableBytes || inspectedByteCount == maximumBytes {
                if firstInspectedByte == 22 {
                    fail(
                        TransparentApplicationClassificationError.tlsClientHelloExceedsClassificationLimit,
                        context: context
                    )
                } else {
                    beginTransition(.opaque, context: context)
                }
            } else if !pendingStageObserved {
                pendingStageObserved = true
                stageObserver?.didEnterTransparentIngressStage(.classificationPending)
            }
        case .failure(let error):
            fail(error, context: context)
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        guard state == .classifying else {
            readComplete = true
            return
        }
        readComplete = true
    }

    func channelInactive(context: ChannelHandlerContext) {
        deadlineTask?.cancel()
        deadlineTask = nil
        state = .terminal
        tlsInspector.removeAll()
        reads.removeAll(keepingCapacity: false)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        deadlineTask?.cancel()
        deadlineTask = nil
        state = .terminal
        context.fireErrorCaught(error)
    }

    private func classify<Bytes: Collection>(
        _ bytes: Bytes
    ) -> Result<TransparentApplicationClassification?, Error> where Bytes.Element == UInt8 {
        guard let firstByte = firstInspectedByte else { return .success(nil) }
        guard firstByte == 22 else {
            switch http1Inspector.append(bytes) {
            case .pending:
                return .success(nil)
            case .recognized:
                return .success(.clearHTTP1)
            case .notHTTP1:
                return .success(.opaque)
            }
        }

        do {
            guard let metadata = try tlsInspector.append(bytes) else {
                return .success(nil)
            }
            if metadata.encryptedClientHelloDetected ||
                metadata.hasALPNExtension && metadata.supportedALPNProtocols.isEmpty {
                return .success(.opaque)
            }
            return .success(.interceptedTLS(metadata))
        } catch {
            return .failure(TransparentApplicationClassificationError.malformedTLSClientHello)
        }
    }

    private func beginTransition(
        _ classification: TransparentApplicationClassification,
        context: ChannelHandlerContext
    ) {
        guard state == .classifying else { return }
        state = .transitioning
        deadlineTask?.cancel()
        deadlineTask = nil
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)

        context.channel
            .setOption(ChannelOptions.autoRead, value: false)
            .flatMap {
                do {
                    return try owner.value.decisionHandler(boundContext.value, classification)
                } catch {
                    return boundContext.value.eventLoop.makeFailedFuture(error)
                }
            }
            .hop(to: context.eventLoop)
            .whenComplete { result in
                guard owner.value.state == .transitioning else { return }
                switch result {
                case .success(let readMode):
                    owner.value.replayAndRemove(readMode: readMode, context: boundContext.value)
                case .failure(let error):
                    owner.value.fail(error, context: boundContext.value)
                }
            }
    }

    private func replayAndRemove(
        readMode: TransparentApplicationReadMode,
        context: ChannelHandlerContext
    ) {
        state = .terminal
        reads.forEach { context.fireChannelRead(wrapInboundOut($0)) }
        if readComplete {
            context.fireChannelReadComplete()
        }
        reads.removeAll(keepingCapacity: false)
        tlsInspector.removeAll()
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
        switch readMode {
        case .automatic:
            let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
            context.channel.setOption(ChannelOptions.autoRead, value: true).whenFailure { error in
                boundContext.value.fireErrorCaught(error)
                boundContext.value.close(promise: nil)
            }
        case .manual:
            context.channel.read()
        }
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        guard state != .terminal else { return }
        state = .terminal
        deadlineTask?.cancel()
        deadlineTask = nil
        reads.removeAll(keepingCapacity: false)
        tlsInspector.removeAll()
        context.fireErrorCaught(error)
        context.close(promise: nil)
    }
}

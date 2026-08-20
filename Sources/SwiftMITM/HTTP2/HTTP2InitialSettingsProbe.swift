import NIOCore

enum HTTP2InitialSettingsProbeError: Error, Equatable {
    case alreadyCompleted
    case bufferLimitExceeded
    case channelClosedBeforeSettings
    case deadlineExceeded
    case enableConnectProtocolDisabledAfterEnabled
    case invalidEnableConnectProtocolValue(UInt32)
    case invalidSettingsLength
    case settingsAcknowledgement
    case settingsStreamIdentifier(Int)
    case unexpectedFrameType(UInt8)
}

struct HTTP2InitialSettingsProbeResult {
    let enablesExtendedConnect: Bool
    let replay: ByteBuffer
}

struct HTTP2InitialSettingsProbe {
    static let maximumBufferedBytes = 64 * 1024

    private struct FrameHeader {
        let payloadLength: Int
        let type: UInt8
        let flags: UInt8
        let streamIdentifier: Int
    }

    enum Outcome {
        case incomplete
        case complete(HTTP2InitialSettingsProbeResult)
    }

    private enum Phase {
        case receiving
        case completed
    }

    private var phase = Phase.receiving
    private var bytes = ByteBuffer()

    mutating func receive(_ input: inout ByteBuffer) throws -> Outcome {
        guard phase == .receiving else {
            throw HTTP2InitialSettingsProbeError.alreadyCompleted
        }
        copyRequiredBytes(from: &input, targetByteCount: 9)

        guard let frameHeader = readFrameHeader() else {
            return .incomplete
        }
        try validate(frameHeader)
        copyRequiredBytes(from: &input, targetByteCount: 9 + frameHeader.payloadLength)
        guard bytes.readableBytes == 9 + frameHeader.payloadLength else {
            return .incomplete
        }

        let enablesExtendedConnect = try readEnableConnectProtocol(payloadLength: frameHeader.payloadLength)
        return complete(enablesExtendedConnect: enablesExtendedConnect)
    }

    private mutating func copyRequiredBytes(from input: inout ByteBuffer, targetByteCount: Int) {
        let missingByteCount = targetByteCount - bytes.readableBytes
        guard missingByteCount > 0 else { return }
        guard var slice = input.readSlice(length: min(missingByteCount, input.readableBytes)) else { return }
        bytes.writeBuffer(&slice)
    }

    private func readFrameHeader() -> FrameHeader? {
        guard bytes.readableBytes >= 9 else { return nil }
        let readerIndex = bytes.readerIndex
        guard
            let lengthHigh = bytes.getInteger(at: readerIndex, as: UInt8.self),
            let lengthMiddle = bytes.getInteger(at: readerIndex + 1, as: UInt8.self),
            let lengthLow = bytes.getInteger(at: readerIndex + 2, as: UInt8.self),
            let frameType = bytes.getInteger(at: readerIndex + 3, as: UInt8.self),
            let flags = bytes.getInteger(at: readerIndex + 4, as: UInt8.self),
            let rawStreamIdentifier = bytes.getInteger(at: readerIndex + 5, endianness: .big, as: UInt32.self)
        else {
            return nil
        }

        return FrameHeader(
            payloadLength: Int(lengthHigh) << 16 | Int(lengthMiddle) << 8 | Int(lengthLow),
            type: frameType,
            flags: flags,
            streamIdentifier: Int(rawStreamIdentifier & 0x7FFF_FFFF)
        )
    }

    private func validate(_ frameHeader: FrameHeader) throws {
        guard frameHeader.payloadLength <= Self.maximumBufferedBytes - 9 else {
            throw HTTP2InitialSettingsProbeError.bufferLimitExceeded
        }
        guard frameHeader.type == 0x4 else {
            throw HTTP2InitialSettingsProbeError.unexpectedFrameType(frameHeader.type)
        }
        guard frameHeader.streamIdentifier == 0 else {
            throw HTTP2InitialSettingsProbeError.settingsStreamIdentifier(frameHeader.streamIdentifier)
        }
        guard frameHeader.flags & 0x1 == 0 else {
            throw HTTP2InitialSettingsProbeError.settingsAcknowledgement
        }
        guard frameHeader.payloadLength.isMultiple(of: 6) else {
            throw HTTP2InitialSettingsProbeError.invalidSettingsLength
        }
    }

    private func readEnableConnectProtocol(payloadLength: Int) throws -> Bool {
        var enablesExtendedConnect = false
        var settingOffset = bytes.readerIndex + 9
        let settingsEnd = settingOffset + payloadLength
        while settingOffset < settingsEnd {
            guard
                let identifier = bytes.getInteger(at: settingOffset, endianness: .big, as: UInt16.self),
                let value = bytes.getInteger(at: settingOffset + 2, endianness: .big, as: UInt32.self)
            else {
                return enablesExtendedConnect
            }
            if identifier == 0x8 {
                guard value <= 1 else {
                    throw HTTP2InitialSettingsProbeError.invalidEnableConnectProtocolValue(value)
                }
                guard !enablesExtendedConnect || value == 1 else {
                    throw HTTP2InitialSettingsProbeError.enableConnectProtocolDisabledAfterEnabled
                }
                enablesExtendedConnect = value == 1
            }
            settingOffset += 6
        }
        return enablesExtendedConnect
    }

    private mutating func complete(enablesExtendedConnect: Bool) -> Outcome {
        phase = .completed
        let result = HTTP2InitialSettingsProbeResult(
            enablesExtendedConnect: enablesExtendedConnect,
            replay: bytes
        )
        bytes.clear()
        return .complete(result)
    }
}

struct HTTP2InitialSettingsProbeBoundary {
    static let deadline = TimeAmount.seconds(5)

    var probe = HTTP2InitialSettingsProbe()
}

final class HTTP2InitialSettingsProbeHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    let capability: EventLoopFuture<Bool>

    private let capabilityPromise: EventLoopPromise<Bool>
    private let deadline: TimeAmount
    private var probe = HTTP2InitialSettingsProbe()
    private var deadlineTask: Scheduled<Void>?
    private var completed = false

    init(eventLoop: EventLoop, deadline: TimeAmount = HTTP2InitialSettingsProbeBoundary.deadline) {
        capabilityPromise = eventLoop.makePromise()
        capability = capabilityPromise.futureResult
        self.deadline = deadline
    }

    func handlerAdded(context: ChannelHandlerContext) {
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
        deadlineTask = context.eventLoop.scheduleTask(in: deadline) {
            owner.value.fail(HTTP2InitialSettingsProbeError.deadlineExceeded, context: boundContext.value)
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        deadlineTask?.cancel()
        deadlineTask = nil
        if !completed {
            failPromise(HTTP2InitialSettingsProbeError.channelClosedBeforeSettings)
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var input = unwrapInboundIn(data)
        do {
            switch try probe.receive(&input) {
            case .incomplete:
                return
            case .complete(let result):
                complete(result, remainingInput: input, context: context)
            }
        } catch {
            fail(error, context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        failPromise(HTTP2InitialSettingsProbeError.channelClosedBeforeSettings)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        failPromise(error)
        context.fireErrorCaught(error)
    }

    private func complete(
        _ result: HTTP2InitialSettingsProbeResult,
        remainingInput: ByteBuffer,
        context: ChannelHandlerContext
    ) {
        guard !completed else { return }
        completed = true
        deadlineTask?.cancel()
        context.fireChannelRead(wrapInboundOut(result.replay))
        if remainingInput.readableBytes > 0 {
            context.fireChannelRead(wrapInboundOut(remainingInput))
        }
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
        capabilityPromise.succeed(result.enablesExtendedConnect)
    }

    private func fail(_ error: Error, context: ChannelHandlerContext) {
        guard !completed else { return }
        completed = true
        deadlineTask?.cancel()
        capabilityPromise.fail(error)
        context.fireErrorCaught(error)
        context.close(promise: nil)
    }

    private func failPromise(_ error: Error) {
        guard !completed else { return }
        completed = true
        deadlineTask?.cancel()
        capabilityPromise.fail(error)
    }
}

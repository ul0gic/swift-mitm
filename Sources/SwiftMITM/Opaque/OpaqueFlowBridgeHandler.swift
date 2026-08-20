import NIOCore

final class OpaqueFlowBridgeHandler {
    private let direction: OpaqueFlowDirection
    private let session: NIOLoopBound<OpaqueCaptureSession>
    private var partner: OpaqueFlowBridgeHandler?
    private var context: ChannelHandlerContext?
    private var pendingRead = false
    private var writable = false
    private var inputEnded = false

    private init(direction: OpaqueFlowDirection, session: NIOLoopBound<OpaqueCaptureSession>) {
        self.direction = direction
        self.session = session
    }

    static func matchedPair(
        flow: CapturedOpaqueFlow,
        sink: CaptureEventSink,
        captureByteLimit: Int = 0,
        eventLoop: EventLoop
    ) -> (OpaqueFlowBridgeHandler, OpaqueFlowBridgeHandler) {
        let session = NIOLoopBound(
            OpaqueCaptureSession(flow: flow, sink: sink, captureByteLimit: captureByteLimit),
            eventLoop: eventLoop
        )
        let client = OpaqueFlowBridgeHandler(direction: .clientToServer, session: session)
        let server = OpaqueFlowBridgeHandler(direction: .serverToClient, session: session)
        client.partner = server
        server.partner = client
        return (client, server)
    }

    func cancel() {
        session.value.close(reason: .cancelled)
        context?.close(promise: nil)
        partner?.close()
    }

    func updateWritability(_ writable: Bool) {
        self.writable = writable
        if writable {
            partner?.resumeRead()
        }
    }

    private func writeToPartner(_ buffer: ByteBuffer) -> Bool {
        guard let partnerContext = partner?.context else { return false }
        partnerContext.write(NIOAny(buffer), promise: nil)
        return true
    }

    private func flushPartner() {
        partner?.context?.flush()
    }

    private func endPartnerOutput() {
        partner?.context?.close(mode: .output, promise: nil)
    }

    private func close() {
        context?.close(promise: nil)
    }

    private func resumeRead() {
        guard pendingRead else { return }
        pendingRead = false
        context?.read()
    }

    private var partnerWritable: Bool {
        partner?.writable ?? false
    }
}

extension OpaqueFlowBridgeHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        session.value.open()
        updateWritability(context.channel.isWritable)
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        writable = false
        self.context = nil
        partner = nil
    }

    func channelActive(context: ChannelHandlerContext) {
        updateWritability(context.channel.isWritable)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        guard session.value.capture(buffer, direction: direction), writeToPartner(buffer) else {
            session.value.fail(reason: .transportFailure)
            context.close(promise: nil)
            partner?.close()
            return
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        flushPartner()
        if partnerWritable {
            context.read()
        } else {
            pendingRead = true
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !inputEnded, partner?.inputEnded == true {
            inputEnded = true
            session.value.directionEnd(direction)
        }
        session.value.fail(reason: .transportFailure)
        partner?.close()
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            guard !inputEnded else { return }
            inputEnded = true
            session.value.directionEnd(direction)
            endPartnerOutput()
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        session.value.fail(reason: .transportFailure)
        context.close(promise: nil)
        partner?.close()
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        updateWritability(context.channel.isWritable)
        context.fireChannelWritabilityChanged()
    }

    func read(context: ChannelHandlerContext) {
        if partnerWritable {
            context.read()
        } else {
            pendingRead = true
        }
    }
}

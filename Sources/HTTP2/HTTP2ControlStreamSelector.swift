import Foundation
import NIOCore
import NIOHTTP2

final class HTTP2ControlStreamSelector: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    private let server: ProxyServer
    private let upstreamMux: NIOHTTP2Handler.StreamMultiplexer
    private let authority: String
    private let target: CapturedTarget?
    private let sink: CaptureEventSink
    private let captureBodyLimit: Int
    private let extendedConnectEnabled: Bool
    private var configuring = false
    private var stopped = false
    private var pending: [HTTP2Frame.FramePayload] = []
    private var bytes = 0
    private var reservations: [HTTPControlByteReservation] = []
    private var admissionReservations: [ResourceReservation] = []

    init(
        server: ProxyServer,
        upstreamMux: NIOHTTP2Handler.StreamMultiplexer,
        authority: String,
        target: CapturedTarget?,
        extendedConnectEnabled: Bool
    ) {
        self.server = server; self.upstreamMux = upstreamMux; self.authority = authority
        self.target = target; self.sink = server.sink; self.captureBodyLimit = server.captureBodyLimit
        self.extendedConnectEnabled = extendedConnectEnabled
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !stopped else { return }
        let payload = unwrapInboundIn(data)
        if configuring {
            do { try retain(payload) }
            catch { fail(context) }
            return
        }
        guard case let .headers(frame) = payload else { fail(context); return }
        let requestID = UUID()
        do {
            if frame.headers.first(name: ":method") == "CONNECT" {
                try configureCONNECT(payload, requestID: requestID, context: context)
                return
            }
            try configureHTTP(payload, requestID: requestID, context: context)
            context.fireChannelRead(data)
            context.pipeline.syncOperations.removeHandler(self, promise: nil)
        } catch {
            server.controlEventSink?.receive(.failure(
                requestID: requestID,
                code: error as? HTTPControlError ?? .unsupportedEdit
            ))
            fail(context)
        }
    }

    private func validateCONNECT(_ payload: HTTP2Frame.FramePayload, requestID: UUID) throws {
        guard case let .headers(frame) = payload else { throw HTTPControlError.invalidConfiguration }
        if let snapshot = server.controlRuntime?.configurationSnapshot {
            let message = HTTPControlMessage(
                context: HTTPControlContext(
                    requestID: requestID,
                    authority: authority,
                    method: "CONNECT",
                    path: frame.headers.first(name: ":path") ?? "",
                    isHTTP2OrLater: true
                ),
                phase: .request,
                headers: frame.headers.filter { !$0.name.hasPrefix(":") }.map {
                    HTTPHeaderField(name: $0.name, value: $0.value)
                }
            )
            switch try snapshot.evaluateHead(message) {
            case let .decision(decision):
                guard case .forward = decision.action, decision.appliedRuleIDs.isEmpty else {
                    throw HTTPControlError.unsupportedEdit
                }
            case .needsBody: throw HTTPControlError.unsupportedEdit
            }
        }
    }

    private func configureCONNECT(
        _ payload: HTTP2Frame.FramePayload,
        requestID: UUID,
        context: ChannelHandlerContext
    ) throws {
        try retain(payload)
        try validateCONNECT(payload, requestID: requestID)
        configuring = true
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        H2StreamGlue.glue(
            inboundStream: context.channel,
            upstreamMux: upstreamMux,
            configuration: .init(
                authority: authority,
                target: target,
                sink: sink,
                captureBodyLimit: captureBodyLimit,
                extendedConnectEnabled: extendedConnectEnabled
            )
        )
            .assumeIsolated()
.whenComplete { result in
                let handler = owner.value
                guard !handler.stopped else { return }
                switch result {
                case .failure: handler.fail(context)
                case .success: handler.handoff(context)
                }
            }
    }

    private func handoff(_ context: ChannelHandlerContext) {
        let frameReservations = reservations
        let frameAdmissionReservations = admissionReservations
        reservations.removeAll()
        admissionReservations.removeAll()
        var frames = pending
        pending.removeAll()
        for frame in frames { context.fireChannelRead(NIOAny(frame)) }
        frames.removeAll()
        frameReservations.forEach { $0.release() }
        frameAdmissionReservations.forEach { $0.release() }
        bytes = 0
        context.fireChannelReadComplete()
        context.pipeline.syncOperations.removeHandler(self, promise: nil)
    }

    private func configureHTTP(
        _ payload: HTTP2Frame.FramePayload,
        requestID: UUID,
        context: ChannelHandlerContext
    ) throws {
        guard case let .headers(frame) = payload else { throw HTTPControlError.invalidConfiguration }
        let scheme = target?.ingressProvenance == .priorKnowledgeHTTP2 ? "http" : "https"
        guard frame.headers.first(name: ":scheme") == scheme,
              let destination = HTTPForwardTarget(scheme: scheme, authority: authority, requestTarget: "/") else {
            throw HTTPControlError.invalidConfiguration
        }
        let errorState = HTTP2StreamErrorState()
        let channel = context.channel
        let server = server
        let sink = sink
        let options = options(destination: destination, requestID: requestID)
        try context.pipeline.syncOperations.addHandlers([
            HTTP2CaptureTapHandler(
                direction: .request,
                requestID: requestID,
                authority: authority,
                target: target,
                sink: sink,
                captureBodyLimit: captureBodyLimit,
                errorState: errorState
            ),
            HTTP2FramePayloadToHTTP1ServerCodec(),
            ExplicitHTTPProxyHandler(
                sink: sink,
                captureBodyLimit: captureBodyLimit,
                connectionID: UUID(),
                connect: { target, loop in
                    server.connectControlledHTTP2(
                        target: target,
                        requestID: requestID,
                        errorState: errorState,
                        downstream: channel,
                        on: loop
                    )
                },
                onCONNECT: { stream, _ in stream.close(promise: nil) },
                options: options
            )
        ])
    }

    private func options(destination: HTTPForwardTarget, requestID: UUID) -> HTTPForwardOptions {
        var options = HTTPForwardOptions()
        options.controlRuntime = server.controlRuntime
        options.controlEventSink = server.controlEventSink
        options.admission = server.admission
        options.fixedDestination = destination
        options.typedHTTP2 = true
        options.externallyCaptured = true
        options.requestID = requestID
        return options
    }

    private func retain(_ payload: HTTP2Frame.FramePayload) throws {
        let size: Int
        switch payload {
        case let .headers(frame):
            size = frame.headers.reduce(64) { $0 + $1.name.utf8.count + $1.value.utf8.count + 32 }
        case let .data(frame): size = frame.data.readableBytes + 64
        default: size = 64
        }
        guard size <= 65_536 - bytes else { throw HTTPControlError.capacityExceeded }
        if let runtime = server.controlRuntime {
            reservations.append(try runtime.reserveTransportBytes(size))
        }
        if let reservation = try server.admission?.acquire(.retainedBytes(size)) {
            admissionReservations.append(reservation)
        }
        bytes += size
        pending.append(payload)
    }

    private func release() {
        reservations.forEach { $0.release() }; reservations.removeAll()
        admissionReservations.forEach { $0.release() }; admissionReservations.removeAll()
        bytes = 0
    }

    private func fail(_ context: ChannelHandlerContext) {
        stopped = true; pending.removeAll(); release(); context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        stopped = true; pending.removeAll(); release(); context.fireChannelInactive()
    }
}

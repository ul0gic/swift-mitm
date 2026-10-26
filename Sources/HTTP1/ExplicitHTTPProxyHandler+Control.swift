import Foundation
import NIOCore
import NIOHTTP1

extension ExplicitHTTPProxyHandler {
    func exchange(requestID: UUID) -> Exchange? {
        for exchange in queue {
            if let exchange, exchange.id == requestID {
                return exchange
            }
        }
        return nil
    }

    func controlMessage(
        _ exchange: Exchange,
        phase: HTTPControlPhase = .request,
        status: Int? = nil,
        headers: HTTPHeaders? = nil,
        body: Data? = nil
    ) -> HTTPControlMessage {
        let identity = HTTPControlContext(
            requestID: exchange.id,
            authority: exchange.originalTarget?.authority ?? exchange.head.uri,
            method: exchange.head.method.rawValue,
            path: exchange.originalTarget?.requestTarget ?? exchange.head.uri,
            isHTTP2OrLater: typedHTTP2
        )
        return HTTPControlMessage(
            context: identity,
            phase: phase,
            status: status,
            headers: Self.fields(headers ?? exchange.head.headers),
            body: body
        )
    }

    func prepareControlledRequest(_ exchange: Exchange) -> Bool {
        guard let runtime = controlRuntime, exchange.head.method != .CONNECT else { return true }
        let snapshot = runtime.configurationSnapshot
        guard !snapshot.rules.isEmpty else { return true }
        exchange.controlSnapshot = snapshot
        let message = controlMessage(exchange)
        do {
            let headResult = try snapshot.evaluateHead(message)
            var reservationBytes = snapshot.retainedBytes + message.retainedBytes + 65_536
            switch headResult {
            case let .needsBody(maximum):
                exchange.controlMaximumBody = maximum
                exchange.controlCollectingBody = true
                exchange.controlReady = false
                reservationBytes += maximum + snapshot.limits.maximumBodyBytes
            case let .decision(decision):
                if case .breakpoint = decision.action {
                    exchange.controlMaximumBody = min(snapshot.limits.maximumBodyBytes, 65_536)
                }
            }
            exchange.controlReservation = try runtime.reserveTransportBytes(reservationBytes)
            exchange.controlAdmissionReservation = try admission?.acquire(.retainedBytes(reservationBytes))
            if let target = exchange.target,
               let original = ConnectionTarget(host: target.host, port: target.port, ingressProvenance: .explicitHTTP) {
                captureRequestHead(exchange, target: original.capturedTarget())
            }
            switch headResult {
            case .needsBody: context?.read()
            case let .decision(decision): applyControlledRequest(decision, exchange: exchange, message: message)
            }
            return !stopped
        } catch {
            controlFailed(exchange, error: error)
            return false
        }
    }

    func retainControlledRequest(_ part: HTTPServerRequestPart, exchange: Exchange) -> Bool {
        guard !exchange.controlReady, case let .body(buffer) = part else { return false }
        guard buffer.readableBytes <= exchange.controlMaximumBody - exchange.controlBody.count else {
            controlFailed(exchange, error: HTTPControlError.bodyLimitExceeded)
            return true
        }
        exchange.controlBody.append(contentsOf: buffer.readableBytesView)
        return true
    }

    func completeControlledRequest(_ exchange: Exchange) {
        guard let snapshot = exchange.controlSnapshot else { return }
        exchange.controlCollectingBody = false
        let message = controlMessage(exchange, body: exchange.controlBody)
        do {
            let decision = try exchange.pendingBodyDecision ?? snapshot.evaluate(message)
            exchange.pendingBodyDecision = nil
            applyControlledRequest(decision, exchange: exchange, message: message)
        } catch { controlFailed(exchange, error: error) }
    }

    func applyControlledRequest(_ decision: HTTPControlDecision, exchange: Exchange, message: HTTPControlMessage) {
        if case .breakpoint = decision.action {
            exchange.controlReady = false
            runControlBreakpoint(decision, message: message, exchange: exchange) { handler, exchange, resolved in
                handler.commitControlledRequest(resolved, exchange: exchange)
            }
            return
        }
        commitControlledRequest(decision, exchange: exchange)
    }

    func commitControlledRequest(_ decision: HTTPControlDecision, exchange: Exchange) {
        guard !stopped, let context else { return }
        do {
            try validateControlledDecision(decision, exchange: exchange, phase: .request)
            exchange.requestFault = try selectedFault(decision, direction: .request)
        } catch {
            controlFailed(exchange, error: error)
            return
        }
        exchange.effectiveHead.headers = HTTPHeaders(decision.headers.map { ($0.name, $0.value) })
        guard applyControlledRequestAction(decision, exchange: exchange) else { return }
        publishEffectiveRequestHead(decision, exchange: exchange)
        exchange.controlReady = true
        if !exchange.controlBody.isEmpty {
            var buffer = context.channel.allocator.buffer(capacity: exchange.controlBody.count)
            buffer.writeBytes(exchange.controlBody)
            exchange.pending.append(.body(buffer))
            exchange.controlBody.removeAll(keepingCapacity: false)
        }
        if exchange.requestEnded {
            exchange.pending.append(.end(exchange.requestControlTrailers))
        }
        if active == nil {
            startNext()
        }
    }

    func applyControlledRequestAction(_ decision: HTTPControlDecision, exchange: Exchange) -> Bool {
        switch decision.action {
        case .abort:
            controlFailed(exchange, error: HTTPControlError.cancelled)
            return false
        case .breakpoint:
            controlFailed(exchange, error: HTTPControlError.unsupportedEdit)
            return false
        case let .route(authority, _):
            guard let original = exchange.target,
                  let target = HTTPForwardTarget(
                      scheme: original.scheme,
                      authority: authority,
                      requestTarget: original.requestTarget
                  ) else {
                controlFailed(exchange, error: HTTPControlError.unsupportedEdit)
                return false
            }
            exchange.target = target
        case .respond:
            publishEffectiveRequestHead(decision, exchange: exchange)
            exchange.syntheticDecision = decision
            exchange.controlReady = true
            if active == nil {
                startNext()
            }
            return false
        case let .replaceBody(body, maximum, _, _):
            if !exchange.requestEnded {
                exchange.controlMaximumBody = maximum
                exchange.pendingBodyDecision = decision
                exchange.controlReady = false
                exchange.controlCollectingBody = true
                context?.read()
                return false
            }
            exchange.controlBody = body
        case .forward, .editHeaders, .fault: break
        }
        return true
    }

    func publishEffectiveRequestHead(_ decision: HTTPControlDecision, exchange: Exchange) {
        guard !exchange.effectiveRequestPublished else { return }
        exchange.effectiveRequestPublished = true
        let headers: [HTTPHeaderField]
        if let target = exchange.target {
            headers = Self.fields(originHead(exchange, target: target).headers)
        } else {
            headers = decision.headers
        }
        controlEventSink?.receive(.effectiveHead(
            requestID: exchange.id,
            phase: .request,
            headers: headers,
            appliedRuleIDs: decision.appliedRuleIDs
        ))
        if decision.overridesNoTransform {
            controlEventSink?.receive(.noTransformOverride(
                requestID: exchange.id,
                phase: .request,
                appliedRuleIDs: decision.appliedRuleIDs
            ))
        }
        (sink as? ObservationCaptureSink)?.receiveEffective(.requestHead(CapturedRequestHead(
            id: exchange.id,
            timestamp: Date(),
            scheme: exchange.target?.scheme ?? "http",
            authority: exchange.target?.authority ?? exchange.head.uri,
            method: exchange.head.method.rawValue,
            path: exchange.target?.requestTarget ?? exchange.head.uri,
            version: typedHTTP2 ? .http2 : .http11,
            headers: headers
        )))
    }

    func controlWorkLifetime(_ exchange: Exchange) -> HTTPControlWorkLifetime {
        HTTPControlWorkLifetime(
            controls: [
                exchange.controlReservation,
                exchange.responseControlReservation,
                exchange.breakpointEditReservation
            ].compactMap(\.self),
            global: [
                exchange.controlAdmissionReservation,
                exchange.responseControlAdmissionReservation,
                exchange.breakpointEditAdmissionReservation
            ].compactMap(\.self)
        )
    }

    func retainControlBytesUntilClosed(_ channel: Channel, exchange: Exchange) {
        guard exchange.controlSnapshot != nil else { return }
        let lifetime = controlWorkLifetime(exchange)
        channel.closeFuture.whenComplete { _ in withExtendedLifetime(lifetime) {} }
    }

    func validateControlledDecision(
        _ decision: HTTPControlDecision,
        exchange: Exchange,
        phase: HTTPControlPhase
    ) throws {
        _ = try HTTPControlConfiguration.headerBytes(decision.headers, multiplexed: typedHTTP2)
        if let synthetic = decision.syntheticResponseHeaders {
            _ = try HTTPControlConfiguration.headerBytes(synthetic, multiplexed: typedHTTP2)
        }
        guard let snapshot = exchange.controlSnapshot else { throw HTTPControlError.cancelled }
        switch decision.action {
        case let .replaceBody(body, maximum, _, override):
            guard maximum <= snapshot.limits.maximumBodyBytes, body.count <= snapshot.limits.maximumBodyBytes,
                  exchange.head.method != .HEAD,
                  exchange.head.method != .CONNECT else { throw HTTPControlError.unsupportedEdit }
            let headers = try controlledOriginalHeaders(exchange, phase: phase)
            guard override || !HTTPControlConfiguration.hasNoTransform(headers)
                else { throw HTTPControlError.noTransform }
            let bytes = phase == .request ? exchange.controlBody.count : exchange.responseControlBody.count
            guard bytes <= maximum else { throw HTTPControlError.bodyLimitExceeded }
        case let .respond(_, _, body):
            guard body.count <= snapshot.limits.maximumBodyBytes,
                  exchange.head.method != .HEAD || body.isEmpty else { throw HTTPControlError.unsupportedEdit }
        default: break
        }
    }

    private func controlledOriginalHeaders(_ exchange: Exchange, phase: HTTPControlPhase) throws -> [HTTPHeaderField] {
        if phase == .request {
            return Self.fields(exchange.head.headers)
        }
        guard let head = exchange.responseControlHead else { throw HTTPControlError.unsupportedEdit }
        return Self.fields(head.headers)
    }

    func selectedFault(_ decision: HTTPControlDecision, direction: HTTPFaultDirection) throws -> HTTPFault? {
        let faults = decision.faults.filter { $0.direction == direction }
        guard let first = faults.first else { return nil }
        var delay = first.delayMilliseconds
        var rate = first.bytesPerSecond
        var burst = first.burstBytes
        var reset = first.resetAfterBytes
        var scope = first.resetScope
        for fault in faults.dropFirst() {
            delay += fault.delayMilliseconds
            burst = min(burst, fault.burstBytes)
            if let candidate = fault.bytesPerSecond {
                rate = rate.map { min($0, candidate) } ?? candidate
            }
            if let candidate = fault.resetAfterBytes {
                if reset == nil || candidate < (reset ?? candidate) {
                    reset = candidate
                    scope = fault.resetScope
                } else if candidate == reset, fault.resetScope == .transport {
                    scope = .transport
                }
            }
        }
        guard delay <= 30_000 else { throw HTTPControlError.invalidConfiguration }
        return try HTTPFault(
            direction: direction,
            delayMilliseconds: delay,
            bytesPerSecond: rate,
            burstBytes: burst,
            resetAfterBytes: reset,
            resetScope: scope
        )
    }

    func sendControlledResponse(status: Int, headers: [HTTPHeaderField], body: Data, exchange: Exchange) {
        guard let context else { return }
        (sink as? ObservationCaptureSink)?.receiveEffective(.requestEnd(requestID: exchange.id, truncated: true))
        controlEventSink?.receive(.effectiveHead(
            requestID: exchange.id,
            phase: .response,
            headers: headers,
            appliedRuleIDs: []
        ))
        let response = HTTPResponseHead(
            version: .http1_1,
            status: HTTPResponseStatus(statusCode: status),
            headers: HTTPHeaders(headers.map { ($0.name, $0.value) })
        )
        (sink as? ObservationCaptureSink)?.receiveEffective(.responseHead(CapturedResponseHead(
            requestID: exchange.id,
            timestamp: Date(),
            status: status,
            version: typedHTTP2 ? .http2 : .http11,
            headers: headers
        )))
        context.write(wrapOutboundOut(.head(response)), promise: nil)
        if !body.isEmpty {
            var buffer = context.channel.allocator.buffer(capacity: body.count)
            buffer.writeBytes(body)
            (sink as? ObservationCaptureSink)?.receiveBody(
                requestID: exchange.id,
                direction: .response,
                representation: .effective,
                bytes: buffer.readableBytesView
            )
            context.write(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: nil)
        }
        (sink as? ObservationCaptureSink)?.receiveEffective(.responseEnd(requestID: exchange.id, truncated: false))
        context.writeAndFlush(wrapOutboundOut(.end(nil))).assumeIsolated().whenComplete { _ in
            self.abort(message: "HTTP control completed with synthetic response")
        }
    }

    func controlFailed(_ exchange: Exchange, error: any Error) {
        controlEventSink?.receive(.failure(
            requestID: exchange.id,
            code: error as? HTTPControlError ?? .unsupportedEdit
        ))
        reject(status: .badGateway, message: "HTTP control rejected exchange")
    }
}

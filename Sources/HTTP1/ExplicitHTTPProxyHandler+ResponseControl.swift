import Foundation
import NIOCore
import NIOHTTP1

extension ExplicitHTTPProxyHandler {
    func prepareControlledResponse(_ head: HTTPResponseHead, exchange: Exchange) -> Bool {
        guard let snapshot = exchange.controlSnapshot else { return false }
        let message = controlMessage(exchange, phase: .response, status: Int(head.status.code), headers: head.headers)
        do {
            switch try snapshot.evaluateHead(message) {
            case let .needsBody(maximum):
                guard let runtime = controlRuntime else { throw HTTPControlError.cancelled }
                let bytes = maximum + snapshot.limits.maximumBodyBytes
                    + snapshot.retainedBytes + message.retainedBytes + 65_536
                exchange.responseControlReservation = try runtime.reserveTransportBytes(bytes)
                exchange.responseControlAdmissionReservation = try admission?.acquire(.retainedBytes(bytes))
                exchange.responseControlHead = head
                exchange.responseControlMaximum = maximum
                exchange.responseControlCollecting = true
            case let .decision(decision):
                if decision.faults.contains(where: { $0.direction == .response }), let runtime = controlRuntime {
                    let bytes = 65_536 + message.retainedBytes + snapshot.retainedBytes
                    exchange.responseControlReservation = try runtime.reserveTransportBytes(bytes)
                    exchange.responseControlAdmissionReservation = try admission?.acquire(.retainedBytes(bytes))
                }
                applyControlledResponse(decision, exchange: exchange, head: head, message: message)
            }
        } catch { controlFailed(exchange, error: error) }
        return true
    }

    func retainControlledResponse(_ body: ByteBuffer, exchange: Exchange) -> Bool {
        guard exchange.responseControlHead != nil else { return false }
        guard body.readableBytes <= exchange.responseControlMaximum - exchange.responseControlBody.count else {
            controlFailed(exchange, error: HTTPControlError.bodyLimitExceeded)
            return true
        }
        exchange.responseControlBody.append(contentsOf: body.readableBytesView)
        return true
    }

    func completeControlledResponse(_ trailers: HTTPHeaders?, exchange: Exchange) {
        guard let head = exchange.responseControlHead else { return }
        exchange.responseControlEnded = true
        exchange.responseControlTrailers = trailers
        if !externallyCaptured, let trailers, !trailers.isEmpty {
            sink.receive(.responseTrailers(requestID: exchange.id, headers: Self.fields(trailers)))
        }
        if !externallyCaptured {
            sink.receive(.responseEnd(requestID: exchange.id, truncated: exchange.responseBody.truncated))
        }
        exchange.responseEnded = true
        correlator.end(id: exchange.id, request: false)
        guard exchange.responseControlCollecting, let snapshot = exchange.controlSnapshot else { return }
        exchange.responseControlCollecting = false
        let message = controlMessage(
            exchange,
            phase: .response,
            status: Int(head.status.code),
            headers: head.headers,
            body: exchange.responseControlBody
        )
        do {
            guard trailers?.isEmpty ?? true else { throw HTTPControlError.unsupportedEdit }
            let decision = try exchange.pendingResponseBodyDecision ?? snapshot.evaluate(message)
            exchange.pendingResponseBodyDecision = nil
            applyControlledResponse(decision, exchange: exchange, head: head, message: message)
        } catch { controlFailed(exchange, error: error) }
    }

    func applyControlledResponse(
        _ decision: HTTPControlDecision,
        exchange: Exchange,
        head: HTTPResponseHead,
        message: HTTPControlMessage
    ) {
        if case .breakpoint = decision.action, let runtime = controlRuntime {
            do {
                let maximum = min(exchange.controlSnapshot?.limits.maximumBodyBytes ?? 0, 65_536)
                let bytes = message.retainedBytes + (exchange.controlSnapshot?.retainedBytes ?? 0) + 65_536
                exchange.responseControlReservation = try runtime.reserveTransportBytes(bytes)
                exchange.responseControlAdmissionReservation = try admission?.acquire(.retainedBytes(bytes))
                exchange.responseControlHead = head
                exchange.responseControlMaximum = maximum
            } catch {
                controlFailed(exchange, error: error)
                return
            }
            runControlBreakpoint(decision, message: message, exchange: exchange) { handler, exchange, resolved in
                handler.commitControlledResponse(resolved, exchange: exchange, head: head)
            }
            return
        }
        commitControlledResponse(decision, exchange: exchange, head: head)
    }

    func commitControlledResponse(_ decision: HTTPControlDecision, exchange: Exchange, head: HTTPResponseHead) {
        guard !stopped, let context else { return }
        do {
            try validateControlledDecision(decision, exchange: exchange, phase: .response)
            exchange.responseFault = try selectedFault(decision, direction: .response)
        } catch {
            controlFailed(exchange, error: error)
            return
        }
        if case let .replaceBody(_, maximum, _, _) = decision.action, !exchange.responseControlEnded {
            exchange.pendingResponseBodyDecision = decision
            exchange.responseControlHead = head
            exchange.responseControlCollecting = true
            exchange.responseControlMaximum = maximum
            upstream?.read()
            return
        }
        var effective = head
        effective.headers = HTTPHeaders(decision.headers.map { ($0.name, $0.value) })
        var replacement: Data?
        switch decision.action {
        case .abort, .breakpoint, .route:
            controlFailed(exchange, error: HTTPControlError.unsupportedEdit)
            return
        case let .replaceBody(body, _, _, _): replacement = body
        case let .respond(status, _, body):
            effective.status = HTTPResponseStatus(statusCode: status)
            replacement = body
            markOriginalResponseReplaced(exchange)
        case .forward, .editHeaders, .fault: break
        }
        exchange.responseControlRuleIDs = decision.appliedRuleIDs
        if decision.overridesNoTransform {
            controlEventSink?.receive(.noTransformOverride(
                requestID: exchange.id,
                phase: .response,
                appliedRuleIDs: decision.appliedRuleIDs
            ))
        }
        if let fault = exchange.responseFault {
            configureResponseFault(
                fault,
                head: effective,
                replacement: replacement,
                exchange: exchange,
                context: context
            )
        } else {
            emitControlledResponse(effective, replacement: replacement, exchange: exchange, context: context)
        }
    }

    func markOriginalResponseReplaced(_ exchange: Exchange) {
        if !exchange.responseControlEnded {
            if !externallyCaptured {
                sink.receive(.streamError(
                    requestID: exchange.id,
                    message: "HTTP control replaced incomplete origin representation"
                ))
            }
            exchange.responseControlEnded = true
            exchange.responseEnded = true
            correlator.end(id: exchange.id, request: false)
        }
    }

    func configureResponseFault(
        _ fault: HTTPFault,
        head effective: HTTPResponseHead,
        replacement: Data?,
        exchange: Exchange,
        context: ChannelHandlerContext
    ) {
        exchange.responseControlHead = effective
        exchange.responseControlMaximum = max(exchange.responseControlMaximum, 65_536)
        exchange.responseFaultWriter = HTTPControlFaultWriter(
            fault: fault,
            channel: context.channel,
            emit: { [weak self, weak exchange] bytes in
                guard let self, let exchange else { return }
                self.writeResponseBody(bytes, exchange: exchange, context: context)
            },
            onDrain: { [weak self] in self?.readOriginIfNeeded() },
            onReset: { [weak self, weak exchange] scope in
                guard let self, let exchange else { return }
                self.resetControlFault(scope, direction: .response, exchange: exchange)
            }
        )
        exchange.responseFaultWriter?.beginHead {
            self.emitControlledResponse(effective, replacement: replacement, exchange: exchange, context: context)
        }
    }

    func emitControlledResponse(
        _ effective: HTTPResponseHead,
        replacement: Data?,
        exchange: Exchange,
        context: ChannelHandlerContext
    ) {
        exchange.responseControlHead = nil
        forwardResponseHead(effective, exchange: exchange, context: context)
        let body = replacement ?? exchange.responseControlBody
        if !body.isEmpty {
            var buffer = context.channel.allocator.buffer(capacity: body.count)
            buffer.writeBytes(body)
            if let writer = exchange.responseFaultWriter {
                do { try writer.enqueueRepresentation(buffer) } catch {
                    controlFailed(exchange, error: error)
                    return
                }
            } else {
                writeResponseBody(buffer, exchange: exchange, context: context)
            }
        }
        exchange.responseControlBody.removeAll(keepingCapacity: false)
        if exchange.responseControlEnded {
            let complete = {
                (self.sink as? ObservationCaptureSink)?.receiveEffective(.responseEnd(
                    requestID: exchange.id,
                    truncated: false
                ))
                context.writeAndFlush(self.wrapOutboundOut(.end(exchange.responseControlTrailers)))
.assumeIsolated()
                    .whenComplete { _ in self.finish(exchange) }
            }
            if let writer = exchange.responseFaultWriter {
                writer.finish(complete)
            } else {
                complete()
            }
        } else if replacement != nil {
            controlFailed(exchange, error: HTTPControlError.unsupportedEdit)
        } else {
            readOriginIfNeeded()
        }
    }

    func writeResponseBody(_ body: ByteBuffer, exchange: Exchange, context: ChannelHandlerContext) {
        if exchange.controlSnapshot != nil {
            (sink as? ObservationCaptureSink)?.receiveBody(
                requestID: exchange.id,
                direction: .response,
                representation: .effective,
                bytes: body.readableBytesView
            )
        }
        context.writeAndFlush(wrapOutboundOut(.body(.byteBuffer(body))), promise: nil)
    }

    func resetControlFault(_ scope: HTTPFaultResetScope, direction: HTTPFaultDirection, exchange: Exchange) {
        if scope == .transport {
            if direction == .request, exchange.upstreamHTTP2 {
                upstream?.parent?.close(promise: nil)
            }
            if direction == .response, typedHTTP2 {
                context?.channel.parent?.close(promise: nil)
            }
        }
        controlEventSink?.receive(.failure(requestID: exchange.id, code: .cancelled))
        self.abort(message: "Intentional HTTP fault reset")
    }
}

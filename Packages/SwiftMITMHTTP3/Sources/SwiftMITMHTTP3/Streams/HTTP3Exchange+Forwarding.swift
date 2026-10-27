import Foundation
import NIOCore
import SwiftMITM

extension HTTP3Exchange {
    func holdInput(_ message: HTTP3Message, flow: HTTP3Flow) throws {
        try requireQueueCapacity(message, flow: flow)
        let lease = message.retainedBytes > 0
            ? try transport.resources.reserve(message.retainedBytes, kind: .forwarding) : nil
        flow.input.append(HTTP3HeldMessage(message: message.compacted(), lease: lease))
        flow.pendingBytes += message.body?.readableBytes ?? 0
        observePending()
    }

    func enqueue(_ message: HTTP3Message, lease: HTTP3StorageLease? = nil, flow: HTTP3Flow) throws {
        try requireQueueCapacity(message, flow: flow)
        let retained: HTTP3StorageLease?
        if let lease {
            retained = lease
        } else if message.retainedBytes > 0 {
            retained = try transport.resources.reserve(message.retainedBytes, kind: .forwarding)
        } else {
            retained = nil
        }
        flow.output.append(HTTP3HeldMessage(message: message.compacted(), lease: retained))
        let count = message.body?.readableBytes ?? 0
        flow.pendingBytes += count
        try flow.fault?.enqueue(bytes: count)
        observePending()
        pump(flow)
    }

    func requireQueueCapacity(_ message: HTTP3Message, flow: HTTP3Flow) throws {
        let bytes = message.body?.readableBytes ?? 0
        guard bytes <= 65_536 - request.pendingBytes - response.pendingBytes,
              flow.input.count + flow.output.count < 256 else { throw HTTP3ProxyError.resourceLimit }
    }

    func observePending() {
        transport.resources.observeForwardingPayload(request.pendingBytes + response.pendingBytes)
    }

    func pump(_ flow: HTTP3Flow) {
        guard !failed, !finished, !flow.writing, flow.timer == nil,
              let destination = flow.destination else { return }
        guard destination.isActive else {
            if !flow.outputEnded {
                fail(HTTP3ProxyError.protocolViolation)
            }
            return
        }
        guard destination.isWritable else { return }
        do {
            if flow.output.isEmpty {
                try generateNext(flow)
            }
            guard !flow.output.isEmpty else { readIfReady(flow); return }
            let allowance = flow.fault?.next(nowNanoseconds: NIODeadline.now().uptimeNanoseconds)
            if let reset = allowance?.reset {
                resetFault(reset, flow: flow); return
            }
            if let delay = allowance?.waitNanoseconds, delay > 0 {
                schedule(flow, after: delay); return
            }
            let held = try nextOutput(flow, allowance: allowance)
            try write(held, flow: flow, destination: destination)
        } catch { fail(error) }
    }

    private func nextOutput(_ flow: HTTP3Flow, allowance: HTTPFaultAllowance?) throws -> HTTP3HeldMessage {
        var held = flow.output.removeFirst()
        if let body = held.message.body, let allowance, body.readableBytes > allowance.bytes {
            var remainder = body
            guard let slice = remainder.readSlice(length: allowance.bytes), allowance.bytes > 0 else {
                throw HTTPControlError.invalidConfiguration
            }
            flow.output.insert(
                HTTP3HeldMessage(message: held.message.replacingBody(remainder), lease: held.lease),
                at: 0
            )
            held.message = held.message.replacingBody(slice)
        }
        return held
    }

    func write(_ held: HTTP3HeldMessage, flow: HTTP3Flow, destination: any Channel) throws {
        let count = held.message.body?.readableBytes ?? 0
        do {
            try effectiveOutput(held.message, flow: flow)
            try flow.fault?.didForward(bytes: count, nowNanoseconds: NIODeadline.now().uptimeNanoseconds)
        } catch {
            flow.pendingBytes -= count
            throw error
        }
        flow.writing = true
        pendingWriteCallbacks += 1
        held.message.write(to: destination).assumeIsolated().whenComplete { result in
            withExtendedLifetime(held.lease) {
                flow.writing = false
                flow.pendingBytes -= count
                self.observePending()
                switch result {
                case let .failure(error): self.fail(error)
                case .success:
                    if !self.failed, held.message.endsMessage {
                        flow.outputEnded = true
                        self.captureEnd(flow, representation: .effective)
                    }
                }
                let bound = NIOLoopBound((self, flow), eventLoop: self.transport.eventLoop)
                self.transport.eventLoop.execute {
                    let (exchange, direction) = bound.value
                    exchange.pendingWriteCallbacks -= 1
                    exchange.pump(direction)
                    exchange.finishIfReady()
                }
            }
        }
    }

    func generateNext(_ flow: HTTP3Flow) throws {
        guard let body = flow.generatedBody else { return }
        let count = min(16_384, body.count - flow.generatedOffset)
        if count > 0 {
            var capacity = 1
            while capacity < count { capacity *= 2 }
            let lease = try transport.resources.reserve(capacity, kind: .forwarding)
            let start = body.startIndex + flow.generatedOffset
            let buffer = ByteBuffer(bytes: body[start..<(start + count)])
            let message: HTTP3Message = flow.isRequest ? .request(.body(buffer)) : .response(.body(buffer))
            try requireQueueCapacity(message, flow: flow)
            flow.output.append(HTTP3HeldMessage(message: message, lease: lease))
            flow.pendingBytes += count
            try flow.fault?.enqueue(bytes: count)
            flow.generatedOffset += count
            observePending()
        } else {
            flow.generatedBody = nil
            if let end = flow.generatedEnd {
                flow.generatedEnd = nil
                try enqueue(end.message, lease: end.lease, flow: flow)
            }
        }
    }

    func schedule(_ flow: HTTP3Flow, after nanoseconds: UInt64) {
        let bound = NIOLoopBound((self, flow), eventLoop: transport.eventLoop)
        flow.timer = transport.eventLoop.scheduleTask(in: .nanoseconds(Int64(min(nanoseconds, UInt64(Int64.max))))) {
            let (exchange, direction) = bound.value
            direction.timer = nil
            exchange.pump(direction)
        }
    }

    func resetFault(_ scope: HTTPFaultResetScope, flow: HTTP3Flow) {
        if scope == .transport {
            transport.resetTransport(requestID: id, direction: flow.direction)
        }
        fail(HTTPControlError.cancelled)
    }
}

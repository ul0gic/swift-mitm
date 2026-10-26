import NIOCore

final class HTTPControlFaultWriter {
    private let channel: Channel
    private let emit: (ByteBuffer) -> Void
    private let onDrain: () -> Void
    private let onReset: (HTTPFaultResetScope) -> Void
    private var schedule: HTTPFaultSchedule
    private var pending: [ByteBuffer] = []
    private var timer: Scheduled<Void>?
    private var terminal: (() -> Void)?
    private var head: (() -> Void)?
    private var representation: ByteBuffer?
    private var cancelled = false

    init(
        fault: HTTPFault,
        channel: Channel,
        emit: @escaping (ByteBuffer) -> Void,
        onDrain: @escaping () -> Void,
        onReset: @escaping (HTTPFaultResetScope) -> Void
    ) {
        self.channel = channel
        self.emit = emit
        self.onDrain = onDrain
        self.onReset = onReset
        schedule = HTTPFaultSchedule(fault: fault, startNanoseconds: NIODeadline.now().uptimeNanoseconds)
    }

    func enqueue(_ buffer: ByteBuffer) throws {
        guard !cancelled else { throw HTTPControlError.cancelled }
        guard buffer.readableBytes > 0 else { return }
        try schedule.enqueue(bytes: buffer.readableBytes)
        pending.append(buffer)
        drain()
    }

    var hasPending: Bool { schedule.pendingBytes > 0 || head != nil || timer != nil || representation != nil }

    func enqueueRepresentation(_ buffer: ByteBuffer) throws {
        guard representation == nil, pending.isEmpty, !cancelled else { throw HTTPControlError.capacityExceeded }
        representation = buffer
        drain()
    }

    func beginHead(_ action: @escaping () -> Void) {
        head = action
        drain()
    }

    func finish(_ action: @escaping () -> Void) {
        guard !cancelled else { return }
        terminal = action
        drain()
    }

    func drain() {
        guard !cancelled, timer == nil, channel.isWritable else { return }
        guard refillRepresentation() else { return }
        let now = NIODeadline.now().uptimeNanoseconds
        let allowance = schedule.next(nowNanoseconds: now)
        if let scope = allowance.reset {
            cancel()
            onReset(scope)
            return
        }
        if pending.isEmpty, representation == nil, head == nil {
            completeDrain()
            return
        }
        if allowance.waitNanoseconds > 0 {
            scheduleWake(after: allowance.waitNanoseconds)
            return
        }
        if let head {
            self.head = nil
            head()
            drain()
            return
        }
        if allowance.bytes > 0, !pending.isEmpty {
            emitPending(maximum: allowance.bytes, now: now)
        } else if pending.isEmpty {
            completeDrain()
        }
    }

    private func refillRepresentation() -> Bool {
        guard pending.isEmpty, var body = representation else { return true }
        let count = min(body.readableBytes, 65_536)
        if count > 0, let slice = body.readSlice(length: count) {
            do { try schedule.enqueue(bytes: count) } catch {
                cancel()
                onReset(.stream)
                return false
            }
            pending.append(slice)
        }
        representation = body.readableBytes > 0 ? body : nil
        return true
    }

    private func emitPending(maximum: Int, now: UInt64) {
        var first = pending.removeFirst()
        let count = min(maximum, first.readableBytes)
        guard let slice = first.readSlice(length: count) else {
            cancel()
            onReset(.stream)
            return
        }
        if first.readableBytes > 0 { pending.insert(first, at: 0) }
        do { try schedule.didForward(bytes: count, nowNanoseconds: now) } catch {
            cancel()
            onReset(.stream)
            return
        }
        emit(slice)
        let owner = NIOLoopBound(self, eventLoop: channel.eventLoop)
        channel.eventLoop.execute { owner.value.drain() }
    }

    private func scheduleWake(after nanoseconds: UInt64) {
        let owner = NIOLoopBound(self, eventLoop: channel.eventLoop)
        let delay = Int64(min(nanoseconds, UInt64(Int64.max)))
        timer = channel.eventLoop.scheduleTask(in: .nanoseconds(delay)) {
            owner.value.timer = nil
            owner.value.drain()
        }
    }

    private func completeDrain() {
        if let terminal {
            self.terminal = nil
            terminal()
        }
        onDrain()
    }

    func cancel() {
        cancelled = true
        timer?.cancel()
        timer = nil
        pending.removeAll()
        terminal = nil
        head = nil
        representation = nil
        schedule.cancel()
    }
}

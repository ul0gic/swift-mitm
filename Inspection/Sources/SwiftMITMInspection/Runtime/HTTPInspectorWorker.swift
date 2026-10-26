import Dispatch
import SwiftMITM

extension HTTPInspector {
    func runTurn() {
        var state = worker.withLock { value in
            let taken = value
            value = Worker()
            return taken
        }
        let stopping = ingress.withLock { $0.stopping }
        if stopping {
            stopWorker(&state)
            return
        }
        applyLosses(&state)
        if state.current == nil {
            state.current = takeInput()
        }
        processCurrent(&state)
        let continuing = state.current != nil
        cleanupTerminals(&state)
        worker.withLock { $0 = state }
        let schedule = ingress.withLock { value in
            if continuing || !value.items.isEmpty || !value.losses.isEmpty || value.stopping {
                return true
            }
            value.running = false
            return false
        }
        if schedule {
            queue.async { self.runTurn() }
        }
    }

    private func stopWorker(_ state: inout Worker) {
        if let current = state.current {
            configuration.budget.release(current.charge)
        }
        for exchange in state.exchanges.values { configuration.budget.release(exchange.charge) }
        state = Worker()
        sequences.withLock { $0 = [:] }
        ingress.withLock { value in
            value.running = false
            value.active = []
            value.failed = []
            value.ended = []
            value.closedSockets = []
            if value.baseReservationActive {
                configuration.budget.release(reservedStorageBytes)
                value.baseReservationActive = false
            }
        }
    }

    private func applyLosses(_ state: inout Worker) {
        let losses = ingress.withLock { value in
            let result = value.losses
            value.losses = [:]
            return result
        }
        for (key, diagnostic) in losses {
            if let body = state.bodies.removeValue(forKey: key) {
                body.fail(diagnostic)
            } else {
                emit(key.context, wire: 0..<0, decoded: 0..<0, chain: [], payload: .diagnostic(diagnostic))
            }
            state.sockets.removeValue(forKey: key)?.terminate()
            if state.current?.key == key {
                if let current = state.current {
                    configuration.budget.release(current.charge)
                }
                state.current = nil
                state.bodyAccepted = false
                state.wsConsumed = 0
            }
            if !ingress.withLock({ $0.active.contains(key.id) }) {
                sequences.withLock { _ = $0.removeValue(forKey: key.id) }
            }
        }
    }

    private func takeInput() -> Item? {
        ingress.withLock { value in
            guard !value.items.isEmpty else { return nil }
            let item = value.items.removeFirst()
            value.queuedBytes -= item.charge
            return item
        }
    }

    private func processCurrent(_ state: inout Worker) {
        if let item = state.current {
            let complete: Bool
            do { complete = try process(item, state: &state) }
            catch {
                fail(item.key, error: error, state: &state)
                complete = true
            }
            if complete {
                configuration.budget.release(item.charge)
                state.current = nil
                state.bodyAccepted = false
                state.wsConsumed = 0
            }
        }
    }

    private func cleanupTerminals(_ state: inout Worker) {
        let terminalIDs = ingress.withLock { value in
            value.active.filter { id in
                guard state.current?.key.id != id,
                    !value.items.contains(where: { $0.key.id == id }) else { return false }
                if value.closedSockets.contains(id) {
                    return true
                }
                guard value.ended.contains(self.makeKey(id, .request)),
                    value.ended.contains(self.makeKey(id, .response)) else { return false }
                return state.exchanges.keys.filter { $0.id == id }.allSatisfy { key in
                    let response = Key(HTTPBodyObservation(
                        requestID: id,
                        direction: .response,
                        representation: key.representation
                    ))
                    return value.ended.contains(key) && value.ended.contains(response)
                }
            }
        }
        for id in terminalIDs where !state.exchanges.contains(where: { $0.key.id == id && $0.value.upgraded }) ||
            ingress.withLock({ $0.closedSockets.contains(id) }) {
            for key in state.bodies.keys.filter({ $0.id == id }) {
                state.bodies.removeValue(forKey: key)?.fail(.incompleteTransport)
            }
            for key in state.sockets.keys.filter({ $0.id == id }) {
                state.sockets.removeValue(forKey: key)?.terminate()
            }
            cleanup(id, state: &state)
        }
    }

    func emit(
        _ context: HTTPBodyObservation,
        wire: Range<UInt64>,
        decoded: Range<UInt64>,
        chain: [ContentCoding],
        payload: InspectionEvent.Payload
    ) {
        let sequence = sequences.withLock { values in
            let next = values[context.requestID, default: 0]
            values[context.requestID] = next + 1
            return next
        }
        ingress.withLock { $0.callbackInFlight = true }
        sink.receive(InspectionEvent(
            requestID: context.requestID,
            direction: context.direction,
            representation: context.representation,
            sequence: sequence,
            wireRange: wire,
            decodedRange: decoded,
            codingChain: chain,
            payload: payload
        ))
        ingress.withLock { $0.callbackInFlight = false }
    }

    func fail(_ key: Key, error: any Error, state: inout Worker) {
        let diagnostic: InspectionDiagnostic
        switch error {
        case let value as ContentDecoderError: diagnostic = .malformedCoding(value)
        case let value as SSEParserError: diagnostic = .malformedSSE(value)
        case let value as WebSocketMessageError: diagnostic = .malformedWebSocket(value)
        case let BodyFailure.unsupportedCoding(value): diagnostic = .unsupportedCoding(value)
        case is InspectionBudgetError: diagnostic = .capacityExceeded
        default: diagnostic = .incompleteTransport
        }
        if let body = state.bodies.removeValue(forKey: key) {
            body.fail(diagnostic)
        } else {
            emit(key.context, wire: 0..<0, decoded: 0..<0, chain: [], payload: .diagnostic(diagnostic))
        }
        state.sockets.removeValue(forKey: key)?.terminate()
        ingress.withLock { _ = $0.failed.insert(key) }
    }

    func process(_ item: Item, state: inout Worker) throws -> Bool {
        switch item.input {
        case let .body(_, bytes):
            return try processBody(bytes, key: item.key, state: &state)
        case .unavailable:
            unavailable(item.key, state: &state)
            return true
        case let .capture(event): return try processCapture(event, key: item.key, state: &state)
        case let .wsStart(frame):
            if state.sockets[item.key] == nil {
                state.sockets[item.key] = try WebSocketMessageDecoder(
                    direction: frame.direction,
                    negotiatedExtensions: state.exchanges[exchangeKey(item.key)]?.extensions ?? [],
                    budget: configuration.budget,
                    maximumMessageBytes: configuration.maximumMessageBytes
                )
            }
            try state.sockets[item.key]?.startFrame(
                opcode: frame.opcode,
                fin: frame.fin,
                compressed: frame.compressed,
                byteCount: frame.byteCount
            )
            return true
        case let .wsBytes(_, _, bytes):
            return try socketBytes(bytes, key: item.key, state: &state)
        case .wsEnd:
            return try endSocketFrame(item.key, state: &state)
        }
    }

    private func processBody(_ bytes: [UInt8], key: Key, state: inout Worker) throws -> Bool {
        guard let body = state.bodies[key] else {
            if key.representation == .effective {
                emit(key.context, wire: 0..<0, decoded: 0..<0, chain: [], payload: .diagnostic(.unavailableInput))
                ingress.withLock { _ = $0.failed.insert(key) }
            }
            return true
        }
        if !state.bodyAccepted {
            try body.accept(bytes)
            state.bodyAccepted = true
        }
        return try body.turn()
    }

    private func unavailable(_ key: Key, state: inout Worker) {
        if let body = state.bodies.removeValue(forKey: key) {
            body.fail(.unavailableInput)
        } else {
            emit(key.context, wire: 0..<0, decoded: 0..<0, chain: [], payload: .diagnostic(.unavailableInput))
        }
        ingress.withLock { _ = $0.failed.insert(key) }
    }

    private func socketBytes(_ bytes: [UInt8], key: Key, state: inout Worker) throws -> Bool {
        guard let decoder = state.sockets[key] else { return true }
        var offsets = state.offsets[key, default: (0, 0)]
        let progress = try bytes.withUnsafeBytes { input in
            try decoder.feed(UnsafeRawBufferPointer(rebasing: input[state.wsConsumed...])) { output in
                try self.socketOutput(
                    output,
                    key: key,
                    wireEnd: offsets.wire + UInt64(bytes.count),
                    offsets: &offsets
                )
            }
        }
        state.wsConsumed += progress.consumedBytes
        if !progress.outputLimit {
            offsets.wire += UInt64(bytes.count)
        }
        state.offsets[key] = offsets
        return !progress.outputLimit && state.wsConsumed == bytes.count
    }

    private func endSocketFrame(_ key: Key, state: inout Worker) throws -> Bool {
        guard let decoder = state.sockets[key] else { return true }
        var offsets = state.offsets[key, default: (0, 0)]
        let progress = try decoder.endFrame { output in
            try self.socketOutput(output, key: key, wireEnd: offsets.wire, offsets: &offsets)
        }
        state.offsets[key] = offsets
        if let message = progress.message {
            emit(
                key.context,
                wire: 0..<offsets.wire,
                decoded: 0..<offsets.decoded,
                chain: [],
                payload: .webSocketMessage(message)
            )
        }
        return !progress.outputLimit
    }

    private func socketOutput(
        _ bytes: UnsafeRawBufferPointer,
        key: Key,
        wireEnd: UInt64,
        offsets: inout (wire: UInt64, decoded: UInt64)
    ) throws {
        let charge = bytes.count * 2
        guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
        defer { configuration.budget.release(charge) }
        let start = offsets.decoded
        offsets.decoded += UInt64(bytes.count)
        emit(
            key.context,
            wire: 0..<wireEnd,
            decoded: start..<offsets.decoded,
            chain: [],
            payload: .webSocketChunk(Array(bytes))
        )
    }
}

import Dispatch
import SwiftMITM

extension HTTPInspector {
    private enum Admission {
        case admitted
        case rejected(schedule: Bool)
    }

    func captureDetails(_ event: CaptureEvent) -> (key: Key, charge: Int)? {
        switch event {
        case .requestHead(let head):
            return (makeKey(head.id, .request), requestCharge(head))
        case .responseHead(let head):
            guard head.status >= 200 || head.status == 101 else { return nil }
            let charge = 256 + head.headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
            return (makeKey(head.requestID, .response), charge)
        default: return metadataDetails(event)
        }
    }

    private func requestCharge(_ head: CapturedRequestHead) -> Int {
        var charge = 256 + head.path.utf8.count + head.scheme.utf8.count
        charge += head.method.utf8.count + head.authority.utf8.count
        charge += head.headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
        if let target = head.target {
            charge += target.destination.address.utf8.count + target.logicalAuthority.utf8.count
            charge += target.tlsServerName?.utf8.count ?? 0
            charge += target.originalClient?.address.utf8.count ?? 0
        }
        return charge
    }

    private func metadataDetails(_ event: CaptureEvent) -> (key: Key, charge: Int)? {
        switch event {
        case let .requestTrailers(id, headers):
            let charge = 256 + headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
            return (makeKey(id, .request), charge)
        case let .responseTrailers(id, headers):
            let charge = 256 + headers.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
            return (makeKey(id, .response), charge)
        case .requestEnd(let id, _): return (makeKey(id, .request), 256)
        case .responseEnd(let id, _): return (makeKey(id, .response), 256)
        case let .streamError(id, message): return (makeKey(id, .response), 256 + message.utf8.count)
        case .webSocketOpen(let id, _, _): return (makeKey(id, .response), 256)
        case let .webSocketClose(id, _, _, reason):
            return (makeKey(id, .response), 256 + (reason?.utf8.count ?? 0))
        default: return nil
        }
    }

    func admit(key: Key, charge: Int, terminal: CaptureEvent? = nil, create: () -> Input) {
        let schedule = ingress.withLock { state in
            guard state.accepting else { return false }
            switch admitExchange(key, state: &state) {
            case .admitted: break
            case .rejected(let schedule): return schedule
            }
            recordTerminal(terminal, key: key, state: &state)
            if state.failed.contains(key) {
                guard terminal != nil, !state.running else { return false }
                state.running = true
                return true
            }
            if state.items.count >= configuration.maximumQueuedItems ||
                charge > configuration.maximumQueuedBytes - state.queuedBytes ||
                !configuration.budget.reserve(charge) {
                dropInput(key, state: &state)
            } else {
                state.items.append(Item(key: key, input: create(), charge: charge))
                state.queuedBytes += charge
            }
            if state.running { return false }
            state.running = true
            return true
        }
        if schedule { queue.async { self.runTurn() } }
    }

    private func admitExchange(_ key: Key, state: inout Ingress) -> Admission {
        guard !state.active.contains(key.id) else { return .admitted }
        if state.active.count >= configuration.maximumActiveBodies {
            state.observationLossDetected = true
            guard state.losses.count < configuration.maximumQueuedItems else { return .rejected(schedule: false) }
            state.losses[key] = .capacityExceeded
            if state.running { return .rejected(schedule: false) }
            state.running = true
            return .rejected(schedule: true)
        }
        state.active.insert(key.id)
        return .admitted
    }

    private func recordTerminal(_ event: CaptureEvent?, key: Key, state: inout Ingress) {
        switch event {
        case .requestEnd?, .responseEnd?: state.ended.insert(key)
        case .streamError?:
            state.ended.insert(Key(HTTPBodyObservation(
                requestID: key.id, direction: .request, representation: key.representation
            )))
            state.ended.insert(Key(HTTPBodyObservation(
                requestID: key.id, direction: .response, representation: key.representation
            )))
        case .webSocketClose?: state.closedSockets.insert(key.id)
        default: break
        }
    }

    private func dropInput(_ key: Key, state: inout Ingress) {
        state.observationLossDetected = true
        state.failed.insert(key)
        state.losses[key] = .capacityExceeded
        let dropped = state.items.filter { $0.key == key }
        state.items.removeAll { $0.key == key }
        for item in dropped {
            state.queuedBytes -= item.charge
            configuration.budget.release(item.charge)
        }
    }
}

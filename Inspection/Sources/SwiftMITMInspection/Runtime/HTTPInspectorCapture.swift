import Foundation
import SwiftMITM

extension HTTPInspector {
    func processCapture(_ event: CaptureEvent, key: Key, state: inout Worker) throws -> Bool {
        switch event {
        case let .requestHead(head):
            try requestHead(head, key: key, state: &state)
        case let .responseHead(head):
            try responseHead(head, key: key, state: &state)
        case let .requestTrailers(_, headers), let .responseTrailers(_, headers):
            try state.bodies[key]?.setTrailers(headers)
        case .requestEnd, .responseEnd:
            return try endBody(key, state: &state)
        case let .streamError(id, _):
            streamError(id, key: key, state: &state)
        case let .webSocketOpen(id, _, _):
            for exchangeKey in state.exchanges.keys.filter({ $0.id == id }) {
                state.exchanges[exchangeKey]?.upgraded = true
            }
        case let .webSocketClose(id, _, _, _):
            closeSocket(id, state: &state)
        default: break
        }
        return true
    }

    private func closeSocket(_ id: UUID, state: inout Worker) {
        for socketKey in state.sockets.keys.filter({ $0.id == id }) {
            state.sockets.removeValue(forKey: socketKey)?.terminate()
            state.offsets.removeValue(forKey: socketKey)
        }
        cleanup(id, state: &state)
    }

    private func requestHead(_ head: CapturedRequestHead, key: Key, state: inout Worker) throws {
        let charge = head.path.utf8.count + 512
        guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
        state.exchanges[exchangeKey(key)] = Exchange(path: head.path, upgraded: false, charge: charge)
        state.bodies[key] = try makeBody(key, headers: head.headers, path: head.path, status: nil)
    }

    private func responseHead(_ head: CapturedResponseHead, key: Key, state: inout Worker) throws {
        if var exchange = state.exchanges[exchangeKey(key)] {
            let extensions = head.headers.filter { $0.name.lowercased() == "sec-websocket-extensions" }.map(\.value)
            let charge = extensions.reduce(0) { $0 + $1.utf8.count }
            guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
            exchange.charge += charge
            exchange.extensions = extensions
            let acceptedWebSocket = head.status == 101 && head.headers.contains {
                $0.name.lowercased() == "upgrade" && $0.value.lowercased() == "websocket"
            }
            exchange.upgraded = exchange.upgraded || acceptedWebSocket
            state.exchanges[exchangeKey(key)] = exchange
        }
        state.bodies[key] = try makeBody(
            key,
            headers: head.headers,
            path: state.exchanges[exchangeKey(key)]?.path ?? "",
            status: head.status
        )
    }

    private func endBody(_ key: Key, state: inout Worker) throws -> Bool {
        state.bodies[key]?.end()
        if try state.bodies[key]?.turn() == false {
            return false
        }
        state.bodies.removeValue(forKey: key)
        if var exchange = state.exchanges[exchangeKey(key)] {
            if key.direction == .request {
                exchange.requestEnded = true
            } else {
                exchange.responseEnded = true
            }
            state.exchanges[exchangeKey(key)] = exchange
            if exchange.requestEnded && exchange.responseEnded && !exchange.upgraded {
                if let removed = state.exchanges.removeValue(forKey: exchangeKey(key)) {
                    configuration.budget.release(removed.charge)
                }
                if !state.exchanges.keys.contains(where: { $0.id == key.id }) {
                    cleanup(key.id, state: &state)
                }
            }
        }
        return true
    }

    private func streamError(_ id: UUID, key: Key, state: inout Worker) {
        for bodyKey in state.bodies.keys.filter({ $0.id == id && $0.representation == key.representation }) {
            state.bodies.removeValue(forKey: bodyKey)?.fail(.incompleteTransport)
        }
        if let removed = state.exchanges.removeValue(forKey: exchangeKey(key)) {
            configuration.budget.release(removed.charge)
        }
        if !state.exchanges.keys.contains(where: { $0.id == id }) {
            cleanup(id, state: &state)
        }
    }

    func makeBody(
        _ key: Key,
        headers: [HTTPHeaderField],
        path: String,
        status: Int?
    ) throws -> HTTPInspectionBody {
        try HTTPInspectionBody(
            context: key.context,
            headers: headers,
            path: path,
            status: status,
            configuration: configuration
        ) { [weak self] context, wire, decoded, chain, payload in
            self?.emit(context, wire: wire, decoded: decoded, chain: chain, payload: payload)
        }
    }

    func cleanup(_ id: UUID, state: inout Worker) {
        let keys = state.exchanges.keys.filter { $0.id == id }
        for key in keys {
            if let exchange = state.exchanges.removeValue(forKey: key) {
                configuration.budget.release(exchange.charge)
            }
        }
        sequences.withLock { _ = $0.removeValue(forKey: id) }
        ingress.withLock { value in
            value.active.remove(id)
            value.failed = value.failed.filter { $0.id != id }
            value.ended = value.ended.filter { $0.id != id }
            value.closedSockets.remove(id)
        }
    }
}

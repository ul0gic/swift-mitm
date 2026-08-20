import NIOHPACK

enum HTTP2ExtendedConnectStreamState: Equatable {
    case ordinaryHTTP
    case candidateExtendedConnect
    case acceptedWebSocket
    case rejectedHTTP
    case ended
    case failed
}

enum HTTP2ExtendedConnectStreamDisposition: Equatable {
    case forwardHTTP
    case awaitFinalResponse
    case openWebSocket
    case forwardWebSocket
    case streamError
}

enum HTTP2StreamDirection {
    case request
    case response
}

struct HTTP2ExtendedConnectStreamCoordinator {
    private(set) var state = HTTP2ExtendedConnectStreamState.ordinaryHTTP
    private var receivedInitialRequest = false
    private var requestEnded = false
    private var responseEnded = false

    mutating func receiveInitialRequest(
        headers: HPACKHeaders,
        endStream: Bool,
        extendedConnectEnabled: Bool
    ) -> HTTP2ExtendedConnectStreamDisposition {
        guard !receivedInitialRequest, state == .ordinaryHTTP else {
            return fail()
        }
        receivedInitialRequest = true

        let pseudoHeaders = pseudoHeaderPairs(headers)
        guard pseudoHeaders.contains(where: { $0.0 == ":protocol" }) else {
            if endStream {
                markEnded(.request)
            }
            return .forwardHTTP
        }
        guard isValidWebSocketExtendedConnect(headers, pseudoHeaders: pseudoHeaders),
            extendedConnectEnabled, !endStream else {
            return fail()
        }

        state = .candidateExtendedConnect
        return .awaitFinalResponse
    }

    mutating func receiveResponse(status: Int, endStream: Bool) -> HTTP2ExtendedConnectStreamDisposition {
        guard receivedInitialRequest else { return fail() }
        switch state {
        case .ordinaryHTTP, .rejectedHTTP:
            if endStream {
                markEnded(.response)
            }
            return .forwardHTTP
        case .candidateExtendedConnect:
            guard (100...599).contains(status), status != 101 else {
                return fail()
            }
            if status < 200 {
                guard !endStream else { return fail() }
                return .awaitFinalResponse
            }
            guard (200...299).contains(status), !endStream else {
                state = .rejectedHTTP
                if endStream {
                    markEnded(.response)
                }
                return .forwardHTTP
            }
            state = .acceptedWebSocket
            return .openWebSocket
        case .acceptedWebSocket, .ended, .failed:
            return fail()
        }
    }

    mutating func receiveData(
        direction: HTTP2StreamDirection,
        endStream: Bool
    ) -> HTTP2ExtendedConnectStreamDisposition {
        guard receivedInitialRequest else { return fail() }
        switch state {
        case .ordinaryHTTP, .rejectedHTTP:
            if endStream {
                markEnded(direction)
            }
            return .forwardHTTP
        case .acceptedWebSocket:
            if endStream {
                markEnded(direction)
            }
            return .forwardWebSocket
        case .candidateExtendedConnect, .ended, .failed:
            return fail()
        }
    }

    mutating func end(_ direction: HTTP2StreamDirection) {
        if state != .failed {
            markEnded(direction)
        }
    }

    mutating func fail() -> HTTP2ExtendedConnectStreamDisposition {
        state = .failed
        return .streamError
    }

    private mutating func markEnded(_ direction: HTTP2StreamDirection) {
        switch direction {
        case .request:
            requestEnded = true
        case .response:
            responseEnded = true
        }
        if requestEnded, responseEnded {
            state = .ended
        }
    }

    private func pseudoHeaderPairs(_ headers: HPACKHeaders) -> [(String, String)] {
        headers.compactMap { name, value, _ in
            name.hasPrefix(":") ? (name, value) : nil
        }
    }

    private func isValidWebSocketExtendedConnect(
        _ headers: HPACKHeaders,
        pseudoHeaders: [(String, String)]
    ) -> Bool {
        let required = [
            ":method": "CONNECT",
            ":protocol": "websocket"
        ]
        let requiredNames = Set([":method", ":protocol", ":scheme", ":path", ":authority"])
        guard pseudoHeaders.count == requiredNames.count,
            Set(pseudoHeaders.map(\.0)) == requiredNames else {
            return false
        }
        guard !headers.contains(where: { name, _, _ in
            let normalizedName = name.lowercased()
            return normalizedName == "connection" || normalizedName == "upgrade"
        }) else {
            return false
        }
        let values = Dictionary(uniqueKeysWithValues: pseudoHeaders)
        return required.allSatisfy { values[$0.key] == $0.value }
            && !(values[":scheme"] ?? "").isEmpty
            && !(values[":path"] ?? "").isEmpty
            && !(values[":authority"] ?? "").isEmpty
    }
}

import NIOCore

extension ExplicitHTTPProxyHandler {
    func runControlBreakpoint(
        _ decision: HTTPControlDecision,
        message: HTTPControlMessage,
        exchange: Exchange,
        completion: @escaping @Sendable (ExplicitHTTPProxyHandler, Exchange, HTTPControlDecision) -> Void
    ) {
        guard let runtime = controlRuntime, let context else {
            controlFailed(exchange, error: HTTPControlError.cancelled)
            return
        }
        let owner = NIOLoopBound(self, eventLoop: context.eventLoop)
        let requestID = exchange.id
        let loop = context.eventLoop
        let sink = controlEventSink
        let snapshot = exchange.controlSnapshot
        let admission = admission
        let lifetime = controlWorkLifetime(exchange)
        exchange.controlTask = Task {
            let result: Result<(HTTPControlDecision, HTTPControlWorkLifetime), any Error>
            do {
                let resolved = try await HTTPControlTransport.resolve(
                    decision,
                    message: message,
                    runtime: runtime,
                    sink: sink,
                    snapshot: snapshot
                )
                let edit = try HTTPControlWorkLifetime(decision: resolved, runtime: runtime, admission: admission)
                result = .success((resolved, edit))
            } catch { result = .failure(error) }
            loop.execute {
                withExtendedLifetime(lifetime) {
                    let handler = owner.value
                    guard !handler.stopped, let exchange = handler.exchange(requestID: requestID) else { return }
                    switch result {
                    case .success(let (resolved, edit)):
                        exchange.breakpointEditReservation = edit.controls.first
                        exchange.breakpointEditAdmissionReservation = edit.global.first
                        completion(handler, exchange, resolved)
                    case .failure(let error): handler.controlFailed(exchange, error: error)
                    }
                }
            }
        }
    }
}

import Foundation
import NIOCore
import NIOQUICHelpers
import SwiftMITM

final class HTTP3Exchange {
    let id = UUID()
    let downstream: HTTP3DownstreamConnection
    let transport: HTTP3Transport
    let request: HTTP3Flow
    let response: HTTP3Flow
    var context: HTTPControlContext?
    var configuration: HTTPControlConfiguration?
    var configurationWork: HTTPControlWorkLifetime?
    var origin: HTTP3OriginStream?
    var openingOrigin = false
    var failed = false
    var originalFailed = false
    var finished = false
    var controlTasks = 0
    var pendingWriteCallbacks = 0
    var closingChannels = 0
    var cleanupStarted = false

    init(channel: any Channel, downstream: HTTP3DownstreamConnection, transport: HTTP3Transport) {
        self.downstream = downstream
        self.transport = transport
        request = HTTP3Flow(direction: .request, source: channel)
        response = HTTP3Flow(direction: .response, destination: channel)
    }

    static func install(
        on channel: any Channel,
        downstream: HTTP3DownstreamConnection,
        transport: HTTP3Transport
    ) -> EventLoopFuture<Void> {
        let exchange = HTTP3Exchange(channel: channel, downstream: downstream, transport: transport)
        var registered = false
        do {
            try transport.registerExchange(id: exchange.id, downstream: downstream) { reason in
                switch reason {
                case .cancelled: exchange.fail(HTTPControlError.cancelled)
                case let .transportFailure(error): exchange.fail(error)
                }
            }
            registered = true
            try channel.pipeline.syncOperations.addHandler(HTTP3RequestHandler(exchange: exchange))
            exchange.readIfReady(exchange.request)
            return channel.eventLoop.makeSucceededVoidFuture()
        } catch {
            if registered {
                exchange.fail(error)
            } else {
                abort(channel)
            }
            return channel.eventLoop.makeFailedFuture(error)
        }
    }

    func fail(_ error: Error) {
        guard !failed, !finished else { return }
        failed = true
        terminateOriginal(failureDescription(error))
        emit(.streamError(requestID: id, message: failureDescription(error)), representation: .effective)
        if let control = error as? HTTPControlError {
            transport.controlEventSink?.receive(.failure(requestID: id, code: control))
        }
        transport.cancelOriginSetup(requestID: id)
        for flow in [request, response] {
            flow.task?.cancel()
            flow.disposeQueuedStorage()
        }
        cancelControlRuntime()
        closeChild(request.source)
        closeChild(response.source)
        cleanupStarted = true
        observePending()
        finishIfReady()
    }

    func closeChild(_ channel: any Channel?) {
        guard let channel else { return }
        closingChannels += 1
        Self.abort(channel)
        channel.closeFuture.assumeIsolated().whenComplete { _ in
            self.closingChannels -= 1
            self.finishIfReady()
        }
    }

    static func abort(_ channel: any Channel) {
        guard let code = QUICApplicationErrorCode(0x10C) else {
            preconditionFailure("HTTP/3 cancellation code must fit a QUIC variable-length integer")
        }
        channel.triggerUserOutboundEvent(QUICResetStreamEvent(code: code), promise: nil)
        channel.triggerUserOutboundEvent(QUICStopSendingEvent(code: code), promise: nil)
        channel.close(promise: nil)
    }

    func finishIfReady() {
        guard !finished, !request.writing, !response.writing, controlTasks == 0,
              pendingWriteCallbacks == 0, closingChannels == 0,
              failed ? cleanupStarted : (
                  request.outputEnded && response.outputEnded && request.inputDisposed && response.inputDisposed
              ) else { return }
        finished = true
        request.work = nil
        response.work = nil
        request.snapshot = nil
        response.snapshot = nil
        request.head = nil
        response.head = nil
        request.headLease = nil
        response.headLease = nil
        request.decision = nil
        response.decision = nil
        configuration = nil
        configurationWork = nil
        request.source = nil
        request.destination = nil
        response.source = nil
        response.destination = nil
        origin = nil
        transport.finishExchange(requestID: id)
    }

    func cancelControlRuntime() {
        guard let runtime = transport.controlRuntime else { return }
        let requestID = id
        controlTasks += 1
        let bound = NIOLoopBound(self, eventLoop: transport.eventLoop)
        let loop = transport.eventLoop
        Task {
            await runtime.cancel(requestID: requestID)
            loop.execute {
                bound.value.controlTasks -= 1
                bound.value.finishIfReady()
            }
        }
    }

    func failureDescription(_ error: Error) -> String {
        if error is HTTPControlError {
            return "HTTP/3 control rejected or cancelled the exchange"
        }
        if error is HTTP3ProxyError {
            return "HTTP/3 transport could not complete the exchange"
        }
        return "HTTP/3 stream failed"
    }
}

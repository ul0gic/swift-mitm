import NIOCore
import SwiftMITM

final class HTTP3ConnectionOwnership {
    private let resources: HTTP3ResourceTracker
    private var leases: [ResourceReservation]
    private var pending = true
    private var deadline: Scheduled<Void>?
    var attached = false
    var failure: HTTP3ProxyError?
    var timedOut: (() -> Void)?

    init(resources: HTTP3ResourceTracker, configuration: HTTP3ProxyConfiguration, destination: String?) throws {
        self.resources = resources
        leases = try resources.acquire(.connections, destination: destination)
        do { try resources.beginHandshake(maximum: configuration.limits.maximumPendingHandshakes) } catch {
            leases.forEach { $0.release() }
            throw error
        }
    }

    func startDeadline(on eventLoop: any EventLoop, duration: Duration) {
        let components = duration.components
        let nanos = components.seconds * 1_000_000_000 + components.attoseconds / 1_000_000_000
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        deadline = eventLoop.scheduleTask(in: .nanoseconds(nanos)) {
            let owner = state.value
            owner.failure = .handshakeTimedOut
            owner.finishHandshake()
            owner.timedOut?()
        }
    }

    func finishHandshake() {
        guard pending else { return }
        pending = false
        deadline?.cancel()
        deadline = nil
        resources.endHandshake()
    }

    func fail(_ error: any Error) {
        if failure ==
            nil {
            failure = HTTP3ProxyError.fromTransport(error) ?? (pending ? .tlsHandshakeFailed : .protocolViolation)
        }
        finishHandshake()
    }

    func release() {
        finishHandshake()
        timedOut = nil
        leases.forEach { $0.release() }
        leases.removeAll()
    }
}

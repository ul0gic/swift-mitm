import Dispatch
import Foundation
import Logging
import NIOCore
import NIOHTTP3
import NIOQUIC
import SwiftMITM

typealias HTTP3NativeHandler = HTTP3ConnectionHandler<HTTP3ObservedStreamCreator>
typealias HTTP3DatagramHandler = QUICHandler<QUICStreamChannels>

final class HTTP3Transport {
    let eventLoop: any EventLoop
    let configuration: HTTP3ProxyConfiguration
    let sink: any CaptureEventSink
    let observationSink: any HTTPObservationSink?
    let controlRuntime: HTTPControlRuntime?
    let controlEventSink: any HTTPControlEventSink?
    let localAdmission: ResourceAdmissionController
    let sharedAdmission: ResourceAdmissionController?
    let resources: HTTP3ResourceTracker
    let logger = Logger(label: "SwiftMITM.HTTP3")
    let resolutionQueue = DispatchQueue(label: "SwiftMITM.HTTP3.resolution")
    var serverHandler: HTTP3DatagramHandler?
    var clientHandler: HTTP3DatagramHandler?
    var clientHandlerIPv6: HTTP3DatagramHandler?
    var serverSocket: any Channel?
    var clientSocket: any Channel?
    var clientSocketIPv6: any Channel?
    var downstreams: [UUID: HTTP3DownstreamConnection] = [:]
    var sessions: [UUID: HTTP3OriginSession] = [:]
    var fixedSessions: [UUID: UUID] = [:]
    var exchanges: [UUID: HTTP3ExchangeRegistration] = [:]
    var connectionStorage: [UUID: EventLoopFuture<Void>] = [:]
    var closing = false
    var listenerCompleted = false
    var pendingResolutions = 0
    var stopPromise: EventLoopPromise<Void>?
    var stopDeadline: Scheduled<Void>?
    var socketsClosed = false
    var disposalComplete = false

    init(
        eventLoop: any EventLoop,
        configuration: HTTP3ProxyConfiguration,
        sink: any CaptureEventSink,
        observationSink: any HTTPObservationSink?,
        controlRuntime: HTTPControlRuntime?,
        controlEventSink: any HTTPControlEventSink?,
        resources: HTTP3ResourceTracker
    ) {
        self.eventLoop = eventLoop
        self.configuration = configuration
        self.sink = sink
        self.observationSink = observationSink
        self.controlRuntime = controlRuntime
        self.controlEventSink = controlEventSink
        self.resources = resources
        localAdmission = resources.local
        sharedAdmission = resources.shared
    }

    func registerExchange(
        id: UUID,
        downstream: HTTP3DownstreamConnection,
        cancel: @escaping (HTTP3ExchangeCancellation) -> Void
    ) throws {
        guard !closing, downstreams[downstream.id] != nil, downstream.channel.isActive else {
            throw CancellationError()
        }
        let count = exchanges.values.filter { $0.downstream.id == downstream.id }.count
        guard count < configuration.limits.maximumRequestStreamsPerConnection else {
            resources.refuse(.local, .requestStreams)
            throw HTTP3ProxyError.resourceLimit
        }
        guard exchanges[id] == nil else { throw HTTP3ProxyError.protocolViolation }
        let leases = try resources.acquire(.requestStreams)
        exchanges[id] = HTTP3ExchangeRegistration(downstream: downstream, leases: leases, cancel: cancel)
    }

    func cancelOriginSetup(requestID: UUID) {
        guard let exchange = exchanges[requestID] else { return }
        exchange.setupCancelled = true
        exchange.setup?.fail(CancellationError())
        guard let id = exchange.sessionID, let session = sessions[id], session.exclusiveRequestID == requestID else {
            return
        }
        session.draining = true
        session.closedIntentionally = true
        session.channel?.close(promise: nil)
    }

    func resetTransport(requestID: UUID, direction: HTTPFaultDirection) {
        guard let exchange = exchanges[requestID] else { return }
        switch direction {
        case .request:
            if let id = exchange.sessionID {
                sessions[id]?.channel?.close(promise: nil)
            }
        case .response:
            exchange.downstream.channel.close(promise: nil)
        }
    }

    func finishExchange(requestID: UUID) {
        guard let exchange = exchanges.removeValue(forKey: requestID) else { return }
        exchange.setupCancelled = true
        exchange.setup?.fail(CancellationError())
        exchange.cancel = nil
        exchange.leases.forEach { $0.release() }
        if let id = exchange.sessionID, let session = sessions[id], session.exclusiveRequestID == requestID {
            session.draining = true
            session.closedIntentionally = true
            session.channel?.close(promise: nil)
        }
        checkQuiescenceLater()
    }

    func cancelExchanges(
        matching predicate: (HTTP3ExchangeRegistration) -> Bool,
        reason: HTTP3ExchangeCancellation
    ) {
        let callbacks = exchanges.values.filter(predicate).compactMap { exchange in
            let callback = exchange.cancel
            exchange.cancel = nil
            exchange.setupCancelled = true
            switch reason {
            case .cancelled: exchange.setup?.fail(CancellationError())
            case let .transportFailure(error): exchange.setup?.fail(error)
            }
            return callback
        }
        callbacks.forEach { $0(reason) }
    }

    func inboundMemoryConfiguration() -> HTTP3InboundMemoryConfiguration {
        let resources = resources
        return HTTP3InboundMemoryConfiguration(
            maximumEncodedFieldSectionBytes: 32_768,
            maximumBytesPerRead: 16_384
        ) { try resources.reserve($0, kind: .parser) }
    }
}

final class HTTP3ExchangeRegistration {
    let downstream: HTTP3DownstreamConnection
    let leases: [ResourceReservation]
    var cancel: ((HTTP3ExchangeCancellation) -> Void)?
    var sessionID: UUID?
    var setupCancelled = false
    var setupStarted = false
    var setup: HTTP3OriginSetup?

    init(
        downstream: HTTP3DownstreamConnection,
        leases: [ResourceReservation],
        cancel: @escaping (HTTP3ExchangeCancellation) -> Void
    ) {
        self.downstream = downstream
        self.leases = leases
        self.cancel = cancel
    }
}

final class HTTP3OriginSession {
    let id = UUID()
    let downstreamID: UUID
    let exclusiveRequestID: UUID?
    let target: CapturedTarget
    let identity: HTTPAuthority
    let address: SocketAddress
    let promise: EventLoopPromise<any Channel>
    let ownership: HTTP3ConnectionOwnership
    var waiters: [UUID: HTTP3OriginSetup] = [:]
    var connectionResult: Result<any Channel, any Error>?
    var channel: any Channel?
    var draining = false
    var connectionCompleted = false
    var closedIntentionally = false

    init(
        downstreamID: UUID,
        exclusiveRequestID: UUID?,
        target: CapturedTarget,
        route: (identity: HTTPAuthority, address: SocketAddress),
        ownership: HTTP3ConnectionOwnership,
        eventLoop: any EventLoop
    ) {
        self.downstreamID = downstreamID
        self.exclusiveRequestID = exclusiveRequestID
        self.target = target
        identity = route.identity
        address = route.address
        self.ownership = ownership
        promise = eventLoop.makePromise(of: (any Channel).self)
    }
}

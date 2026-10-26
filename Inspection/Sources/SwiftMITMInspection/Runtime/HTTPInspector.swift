import Dispatch
import Foundation
import SwiftMITM
import Synchronization

public final class HTTPInspector: HTTPObservationSink, WebSocketObservationSink {
    struct Key: Hashable, Sendable {
        let id: UUID
        let direction: HTTPObservationDirection
        let representation: HTTPRepresentationKind

        init(_ context: HTTPBodyObservation) {
            id = context.requestID
            direction = context.direction
            representation = context.representation
        }

        var context: HTTPBodyObservation {
            HTTPBodyObservation(requestID: id, direction: direction, representation: representation)
        }
    }

    struct FrameStart: Sendable {
        let direction: WebSocketDirection
        let opcode: WebSocketOpcode
        let fin: Bool
        let compressed: Bool
        let byteCount: Int
    }

    enum Input: Sendable {
        case capture(CaptureEvent)
        case body(HTTPBodyObservation, [UInt8])
        case unavailable(HTTPBodyObservation)
        case wsStart(FrameStart)
        case wsBytes(UUID, WebSocketDirection, [UInt8])
        case wsEnd(UUID, WebSocketDirection)
    }

    struct Item: Sendable {
        let key: Key
        let input: Input
        let charge: Int
    }

    struct Ingress: Sendable {
        var items: [Item] = []
        var losses: [Key: InspectionDiagnostic] = [:]
        var failed: Set<Key> = []
        var active: Set<UUID> = []
        var queuedBytes = 0
        var running = false
        var accepting = true
        var stopping = false
        var callbackInFlight = false
        var ended: Set<Key> = []
        var closedSockets: Set<UUID> = []
        var baseReservationActive = true
        var observationLossDetected = false
    }

    struct Exchange {
        var path: String
        var extensions: [String] = []
        var upgraded: Bool
        var charge: Int
        var requestEnded = false
        var responseEnded = false
    }

    struct Worker {
        var bodies: [Key: HTTPInspectionBody] = [:]
        var exchanges: [Key: Exchange] = [:]
        var sockets: [Key: WebSocketMessageDecoder] = [:]
        var offsets: [Key: (wire: UInt64, decoded: UInt64)] = [:]
        var current: Item?
        var bodyAccepted = false
        var wsConsumed = 0
    }

    let configuration: InspectionConfiguration
    let sink: any InspectionEventSink
    let ingress = Mutex(Ingress())
    let worker = Mutex(Worker())
    let sequences = Mutex<[UUID: UInt64]>([:])
    let queue = DispatchQueue(label: "SwiftMITMInspection.worker")
    public let reservedStorageBytes: Int

    public init(configuration: InspectionConfiguration, sink: any InspectionEventSink) throws {
        let charge = configuration.maximumQueuedItems * 512 + configuration.maximumActiveBodies * 4096
        guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
        reservedStorageBytes = charge
        self.configuration = configuration
        self.sink = sink
    }

    deinit {
        ingress.withLock { state in
            for item in state.items { configuration.budget.release(item.charge) }
            if state.baseReservationActive {
                configuration.budget.release(reservedStorageBytes)
            }
        }
        worker.withLock { state in
            if let current = state.current {
                configuration.budget.release(current.charge)
            }
            for exchange in state.exchanges.values { configuration.budget.release(exchange.charge) }
        }
    }

    public func receive(_ event: CaptureEvent) {
        receive(event, representation: .original)
    }

    public func receive(_ event: CaptureEvent, representation: HTTPRepresentationKind) {
        guard let (key, charge) = captureDetails(event) else { return }
        let represented = Key(HTTPBodyObservation(
            requestID: key.id,
            direction: key.direction,
            representation: representation
        ))
        admit(key: represented, charge: charge, terminal: event) { .capture(event) }
    }

    public func receiveBody(_ observation: HTTPBodyObservation, bytes: UnsafeRawBufferPointer) {
        var offset = 0
        while offset < bytes.count {
            let count = min(65_536, bytes.count - offset)
            let slice = UnsafeRawBufferPointer(rebasing: bytes[offset..<(offset + count)])
            admit(key: Key(observation), charge: count * 2 + 256) { .body(observation, Array(slice)) }
            offset += count
        }
    }

    public func bodyUnavailable(_ observation: HTTPBodyObservation) {
        admit(key: Key(observation), charge: 256) { .unavailable(observation) }
    }

    public func webSocketFrameStarted(
        connectionID: UUID,
        direction: WebSocketDirection,
        opcode: WebSocketOpcode,
        fin: Bool,
        compressed: Bool,
        byteCount: Int
    ) {
        admit(key: socketKey(connectionID, direction), charge: 256) {
            .wsStart(FrameStart(
                direction: direction,
                opcode: opcode,
                fin: fin,
                compressed: compressed,
                byteCount: byteCount
            ))
        }
    }

    public func webSocketFrameBytes(
        connectionID: UUID,
        direction: WebSocketDirection,
        bytes: UnsafeRawBufferPointer
    ) {
        var offset = 0
        while offset < bytes.count {
            let count = min(65_536, bytes.count - offset)
            let slice = UnsafeRawBufferPointer(rebasing: bytes[offset..<(offset + count)])
            admit(key: socketKey(connectionID, direction), charge: count * 2 + 256) {
                .wsBytes(connectionID, direction, Array(slice))
            }
            offset += count
        }
    }

    public func webSocketFrameEnded(connectionID: UUID, direction: WebSocketDirection) {
        admit(key: socketKey(connectionID, direction), charge: 256) { .wsEnd(connectionID, direction) }
    }

    public var observationLossDetected: Bool {
        ingress.withLock { $0.observationLossDetected }
    }

    public func drain(timeout: Duration = .seconds(5)) async -> InspectionDrainResult {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: timeout)
        while true {
            let result = ingress.withLock { state in
                InspectionDrainResult(
                    drained: !state.running && state.items.isEmpty && state.losses.isEmpty,
                    queuedItems: state.items.count + state.losses.count,
                    retainedBytes: configuration.budget.retainedBytes,
                    callbackInFlight: state.callbackInFlight,
                    observationLossDetected: state.observationLossDetected
                )
            }
            if result.drained || clock.now >= deadline {
                return result
            }
            do { try await Task.sleep(for: .milliseconds(10)) }
            catch { return result }
        }
    }

    public func shutdown(timeout: Duration = .seconds(5)) async -> InspectionDrainResult {
        let schedule = ingress.withLock { state in
            state.accepting = false
            state.stopping = true
            for item in state.items { configuration.budget.release(item.charge) }
            state.items = []
            state.queuedBytes = 0
            state.losses = [:]
            if state.running {
                return false
            }
            state.running = true
            return true
        }
        if schedule {
            queue.async { self.runTurn() }
        }
        return await drain(timeout: timeout)
    }

    func makeKey(_ id: UUID, _ direction: HTTPObservationDirection) -> Key {
        Key(HTTPBodyObservation(requestID: id, direction: direction))
    }

    func socketKey(_ id: UUID, _ direction: WebSocketDirection) -> Key {
        makeKey(id, direction == .clientToServer ? .request : .response)
    }

    func exchangeKey(_ key: Key) -> Key {
        Key(HTTPBodyObservation(requestID: key.id, direction: .request, representation: key.representation))
    }
}

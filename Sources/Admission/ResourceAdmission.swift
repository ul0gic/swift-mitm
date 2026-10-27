import NIOConcurrencyHelpers

public enum ResourceAdmissionError: Error, Sendable {
    case invalidConfiguration
    case capacityExceeded
}

public struct ResourceAdmissionConfiguration: Sendable {
    public let maximumConnections: Int
    public let maximumStreams: Int
    public let maximumConnectionsPerDestination: Int
    public let maximumRetainedBytes: Int
    public let maximumReplayAttempts: Int

    public init(
        maximumConnections: Int = 256,
        maximumStreams: Int = 1024,
        maximumConnectionsPerDestination: Int = 64,
        maximumRetainedBytes: Int = 64 * 1024 * 1024,
        maximumReplayAttempts: Int = 8
    ) throws {
        guard (1...4096).contains(maximumConnections),
              (1...16_384).contains(maximumStreams),
              (1...1024).contains(maximumConnectionsPerDestination),
              (1...1_073_741_824).contains(maximumRetainedBytes),
              (1...64).contains(maximumReplayAttempts) else {
            throw ResourceAdmissionError.invalidConfiguration
        }
        self.maximumConnections = maximumConnections
        self.maximumStreams = maximumStreams
        self.maximumConnectionsPerDestination = maximumConnectionsPerDestination
        self.maximumRetainedBytes = maximumRetainedBytes
        self.maximumReplayAttempts = maximumReplayAttempts
    }
}

public struct ResourceAdmissionUsage: Sendable {
    public let connections: Int
    public let streams: Int
    public let retainedBytes: Int
    public let replayAttempts: Int
}

public final class ResourceAdmissionController: Sendable {
    enum Kind: Sendable {
        case connection(destination: String?)
        case stream
        case retainedBytes(Int)
        case replay
    }

    private struct State {
        var connections = 0
        var streams = 0
        var retainedBytes = 0
        var replayAttempts = 0
        var destinations: [String: Int] = [:]
    }

    public let configuration: ResourceAdmissionConfiguration
    private let state = NIOLockedValueBox(State())

    public init(configuration: ResourceAdmissionConfiguration) {
        self.configuration = configuration
    }

    public var usage: ResourceAdmissionUsage {
        state.withLockedValue {
            ResourceAdmissionUsage(
                connections: $0.connections,
                streams: $0.streams,
                retainedBytes: $0.retainedBytes,
                replayAttempts: $0.replayAttempts
            )
        }
    }

    public func makeRetainedByteAccount() -> ResourceRetainedByteAccount {
        ResourceRetainedByteAccount(controller: self)
    }

    public func acquireConnection(destination: String? = nil) throws -> ResourceReservation {
        try acquire(.connection(destination: destination))
    }

    public func acquireStream() throws -> ResourceReservation {
        try acquire(.stream)
    }

    fileprivate func reserveRetainedBytes(_ bytes: Int) -> Bool {
        state.withLockedValue { state in
            Self.reserveRetainedBytes(
                bytes,
                used: &state.retainedBytes,
                maximum: configuration.maximumRetainedBytes
            )
        }
    }

    private static func reserveRetainedBytes(_ bytes: Int, used: inout Int, maximum: Int) -> Bool {
        guard bytes >= 0, bytes <= maximum - used else { return false }
        used += bytes
        return true
    }

    func acquire(_ kind: Kind) throws -> ResourceReservation {
        try state.withLockedValue { state in
            switch kind {
            case let .connection(destination):
                guard state.connections < configuration.maximumConnections else {
                    throw ResourceAdmissionError.capacityExceeded
                }
                if let destination {
                    let count = state.destinations[destination, default: 0]
                    guard count < configuration.maximumConnectionsPerDestination else {
                        throw ResourceAdmissionError.capacityExceeded
                    }
                    state.destinations[destination] = count + 1
                }
                state.connections += 1
            case .stream:
                guard state.streams < configuration.maximumStreams else {
                    throw ResourceAdmissionError.capacityExceeded
                }
                state.streams += 1
            case let .retainedBytes(bytes):
                guard Self.reserveRetainedBytes(
                    bytes,
                    used: &state.retainedBytes,
                    maximum: configuration.maximumRetainedBytes
                ) else {
                    throw ResourceAdmissionError.capacityExceeded
                }
            case .replay:
                guard state.replayAttempts < configuration.maximumReplayAttempts else {
                    throw ResourceAdmissionError.capacityExceeded
                }
                state.replayAttempts += 1
            }
        }
        return ResourceReservation(controller: self, kind: kind)
    }

    fileprivate func release(_ kind: Kind) {
        state.withLockedValue { state in
            switch kind {
            case let .connection(destination):
                state.connections -= 1
                if let destination, let count = state.destinations[destination] {
                    if count == 1 {
                        state.destinations.removeValue(forKey: destination)
                    } else {
                        state.destinations[destination] = count - 1
                    }
                }
            case .stream: state.streams -= 1
            case let .retainedBytes(bytes): state.retainedBytes -= bytes
            case .replay: state.replayAttempts -= 1
            }
        }
    }
}

public final class ResourceRetainedByteAccount: Sendable {
    private let controller: ResourceAdmissionController
    /// Acquisition order is budget, account, controller; the controller never calls back into owners.
    private let used = NIOLockedValueBox(0)

    fileprivate init(controller: ResourceAdmissionController) {
        self.controller = controller
    }

    public var retainedBytes: Int {
        used.withLockedValue { $0 }
    }

    public func reserve(_ bytes: Int) -> Bool {
        guard bytes >= 0 else { return false }
        return used.withLockedValue { count in
            guard controller.reserveRetainedBytes(bytes) else { return false }
            count += bytes
            return true
        }
    }

    public func release(_ bytes: Int) {
        used.withLockedValue { count in
            precondition(bytes >= 0 && bytes <= count)
            controller.release(.retainedBytes(bytes))
            count -= bytes
        }
    }

    deinit {
        let outstanding = used.withLockedValue { count in
            let outstanding = count
            count = 0
            return outstanding
        }
        controller.release(.retainedBytes(outstanding))
    }
}

public final class ResourceReservation: Sendable {
    private let controller: ResourceAdmissionController
    private let kind: ResourceAdmissionController.Kind
    private let released = NIOLockedValueBox(false)

    init(controller: ResourceAdmissionController, kind: ResourceAdmissionController.Kind) {
        self.controller = controller
        self.kind = kind
    }

    public func release() {
        let shouldRelease = released.withLockedValue { released in
            guard !released else { return false }
            released = true
            return true
        }
        if shouldRelease {
            controller.release(kind)
        }
    }

    deinit { release() }
}

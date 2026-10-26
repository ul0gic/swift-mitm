import Foundation
import NIOConcurrencyHelpers
import NIOCore

public enum ReplayError: Error, Sendable {
    case invalidConfiguration
    case incompleteBody
    case bodyLimit
    case invalidRequest
    case unsupportedSession
    case credentialScopeMismatch
    case capacityExceeded
    case incompleteResponse
}

public struct ReplayConfiguration: Sendable {
    public let maximumConcurrentAttempts: Int
    public let maximumBodyBytes: Int

    public init(maximumConcurrentAttempts: Int = 8, maximumBodyBytes: Int = 1_048_576) throws {
        guard (1...64).contains(maximumConcurrentAttempts),
              (1...16_777_216).contains(maximumBodyBytes)
        else { throw ReplayError.invalidConfiguration }
        self.maximumConcurrentAttempts = maximumConcurrentAttempts
        self.maximumBodyBytes = maximumBodyBytes
    }

    private init(validated: Void) {
        maximumConcurrentAttempts = 8
        maximumBodyBytes = 1_048_576
    }

    public static let `default` = ReplayConfiguration(validated: ())
}

public enum ReplayCredentialPolicy: Sendable {
    case strip
    case preserveForSameOrigin(sourceURL: URL)
}

public struct ReplaySource: Sendable {
    public let requestID: UUID
    public let representation: HTTPRepresentationKind

    public init(requestID: UUID, representation: HTTPRepresentationKind) {
        self.requestID = requestID
        self.representation = representation
    }
}

public struct ReplayRequest: Sendable {
    public let source: ReplaySource
    public let destination: URL
    public let method: String
    public let headers: [HTTPHeaderField]
    public let body: [UInt8]
    public let bodyIsComplete: Bool
    public let credentialPolicy: ReplayCredentialPolicy
    public let sensitiveHeaderNames: Set<String>
    public let useControls: Bool

    public init(
        source: ReplaySource,
        destination: URL,
        method: String,
        headers: [HTTPHeaderField],
        body: [UInt8],
        bodyIsComplete: Bool,
        credentialPolicy: ReplayCredentialPolicy = .strip,
        sensitiveHeaderNames: Set<String> = [],
        useControls: Bool = false
    ) {
        self.source = source
        self.destination = destination
        self.method = method
        self.headers = headers
        self.body = body
        self.bodyIsComplete = bodyIsComplete
        self.credentialPolicy = credentialPolicy
        self.sensitiveHeaderNames = sensitiveHeaderNames
        self.useControls = useControls
    }
}

public struct ReplayResult: Sendable {
    public let attemptID: UUID
    public let requestID: UUID
    public let sourceRequestID: UUID
    public let sourceRepresentation: HTTPRepresentationKind
    public let status: Int
    public let responseBodyBytes: Int
    public let timings: HARExchangeTiming?
    public let timingUnavailable: ReplayTimingUnavailable?
    public let responseContentBytes: Int?
}

public enum ReplayTimingUnavailable: Sendable {
    case syntheticResponse
    case incompleteUpload
    case overlappingExchange
}

public enum ReplayResponseEvent: Sendable {
    case started(
        attemptID: UUID,
        requestID: UUID,
        sourceRequestID: UUID,
        sourceRepresentation: HTTPRepresentationKind
    )
    case head(attemptID: UUID, head: CapturedResponseHead)
    case body(attemptID: UUID, requestID: UUID, bytes: [UInt8])
    case trailers(attemptID: UUID, requestID: UUID, headers: [HTTPHeaderField])
    case end(ReplayResult)
}

public protocol ReplayResponseSink: Sendable {
    func receive(_ event: ReplayResponseEvent) async throws
}

final class ReplayCoordinator: Sendable {
    let configuration: ReplayConfiguration
    private let count = NIOLockedValueBox(0)

    init(configuration: ReplayConfiguration) { self.configuration = configuration }

    func acquire() throws -> ReplayAttemptReservation {
        try count.withLockedValue { count in
            guard count < configuration.maximumConcurrentAttempts else { throw ReplayError.capacityExceeded }
            count += 1
        }
        return ReplayAttemptReservation(coordinator: self)
    }

    fileprivate func release() { count.withLockedValue { $0 -= 1 } }
}

final class ReplayAttemptReservation: Sendable {
    private let coordinator: ReplayCoordinator
    private let released = NIOLockedValueBox(false)

    init(coordinator: ReplayCoordinator) { self.coordinator = coordinator }

    func release() {
        if released.withLockedValue({ value in
            guard !value else { return false }
            value = true
            return true
        }) {
            coordinator.release()
        }
    }

    deinit { release() }
}

final class ReplayCancellation: Sendable {
    private struct State {
        var channel: Channel?
        var cancelled = false
    }

    private let setup: ProxyConnectionSetupToken
    private let state = NIOLockedValueBox(State())

    init(setup: ProxyConnectionSetupToken) { self.setup = setup }

    func attach(_ channel: Channel) {
        let cancelled = state.withLockedValue { state in
            state.channel = channel
            return state.cancelled
        }
        if cancelled {
            channel.close(promise: nil)
        }
        setup.complete()
    }

    func cancel() {
        setup.cancel()
        let channel = state.withLockedValue { state in
            state.cancelled = true
            return state.channel
        }
        channel?.close(promise: nil)
    }
}

import NIOConcurrencyHelpers
import NIOCore
import NIOPosix

enum LeafIdentityCacheError: Error, Equatable, Sendable {
    case pendingCapacityExceeded
}

final class LeafIdentityCache: Sendable {
    typealias Mint = @Sendable (String) throws -> MintedIdentity

    struct Snapshot: Equatable, Sendable {
        let cachedHosts: [String]
        let pendingHostCount: Int
    }

    private struct State {
        var identities: [String: MintedIdentity] = [:]
        var recency: [String] = []
        var waiters: [String: [EventLoopPromise<MintedIdentity>]] = [:]
    }

    private enum Lookup {
        case cached(MintedIdentity)
        case pending(EventLoopPromise<MintedIdentity>)
        case start(EventLoopPromise<MintedIdentity>)
        case rejected
    }

    private let threadPool: NIOThreadPool
    private let maximumEntries: Int
    private let maximumPendingMints: Int
    private let mint: Mint
    private let state = NIOLockedValueBox(State())

    init(
        authority: CertificateAuthority,
        threadPool: NIOThreadPool,
        maximumEntries: Int,
        maximumPendingMints: Int
    ) {
        self.threadPool = threadPool
        self.maximumEntries = maximumEntries
        self.maximumPendingMints = maximumPendingMints
        self.mint = { try authority.mintIdentity(forHost: $0) }
        precondition(maximumEntries > 0)
        precondition(maximumPendingMints > 0)
    }

    init(
        threadPool: NIOThreadPool,
        maximumEntries: Int,
        maximumPendingMints: Int,
        mint: @escaping Mint
    ) {
        self.threadPool = threadPool
        self.maximumEntries = maximumEntries
        self.maximumPendingMints = maximumPendingMints
        self.mint = mint
        precondition(maximumEntries > 0)
        precondition(maximumPendingMints > 0)
    }

    func identity(forHost host: String, on eventLoop: EventLoop) -> EventLoopFuture<MintedIdentity> {
        let host = host.lowercased()
        let promise = eventLoop.makePromise(of: MintedIdentity.self)
        let lookup = state.withLockedValue { state -> Lookup in
            if let identity = state.identities[host] {
                Self.recordAccess(to: host, state: &state)
                return .cached(identity)
            }
            if state.waiters[host] != nil {
                state.waiters[host]?.append(promise)
                return .pending(promise)
            }
            guard state.waiters.count < maximumPendingMints else {
                return .rejected
            }
            state.waiters[host] = [promise]
            return .start(promise)
        }

        switch lookup {
        case .cached(let identity):
            promise.succeed(identity)
            return promise.futureResult
        case .pending(let promise):
            return promise.futureResult
        case .start(let promise):
            threadPool
                .runIfActive(eventLoop: eventLoop) { [mint] in try mint(host) }
                .whenComplete { [self] result in complete(host: host, result: result) }
            return promise.futureResult
        case .rejected:
            promise.fail(LeafIdentityCacheError.pendingCapacityExceeded)
            return promise.futureResult
        }
    }

    func snapshot() -> Snapshot {
        state.withLockedValue { state in
            Snapshot(cachedHosts: state.recency, pendingHostCount: state.waiters.count)
        }
    }

    private func complete(host: String, result: Result<MintedIdentity, Error>) {
        let waiters = state.withLockedValue { state -> [EventLoopPromise<MintedIdentity>] in
            let waiters = state.waiters.removeValue(forKey: host) ?? []
            if case .success(let identity) = result {
                state.identities[host] = identity
                Self.recordAccess(to: host, state: &state)
                while state.identities.count > maximumEntries, let evicted = state.recency.first {
                    state.recency.removeFirst()
                    state.identities.removeValue(forKey: evicted)
                }
            }
            return waiters
        }
        waiters.forEach { $0.completeWith(result) }
    }

    private static func recordAccess(to host: String, state: inout State) {
        state.recency.removeAll { $0 == host }
        state.recency.append(host)
    }
}

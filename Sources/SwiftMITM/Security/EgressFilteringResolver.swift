import NIOCore
import NIOPosix

final class EgressFilteringResolver: Resolver, Sendable {
    private let resolver: Resolver & Sendable
    private let policy: EgressPolicy

    init(resolver: Resolver & Sendable, policy: EgressPolicy) {
        self.resolver = resolver
        self.policy = policy
    }

    func initiateAQuery(host: String, port: Int) -> EventLoopFuture<[SocketAddress]> {
        resolver.initiateAQuery(host: host, port: port).map { [policy] addresses in
            addresses.filter { !policy.denies($0) }
        }
    }

    func initiateAAAAQuery(host: String, port: Int) -> EventLoopFuture<[SocketAddress]> {
        resolver.initiateAAAAQuery(host: host, port: port).map { [policy] addresses in
            addresses.filter { !policy.denies($0) }
        }
    }

    func cancelQueries() {
        resolver.cancelQueries()
    }
}

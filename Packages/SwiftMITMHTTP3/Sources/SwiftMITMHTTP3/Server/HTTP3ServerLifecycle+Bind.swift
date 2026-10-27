import NIOCore
import NIOQUIC
import SwiftMITM

extension HTTP3ServerLifecycle {
    func resolveBind(host: String, port: Int) async throws -> SocketAddress {
        try await withCheckedThrowingContinuation { continuation in
            resolutionQueue.async {
                do { continuation.resume(returning: try SocketAddress.makeAddressResolvingHost(host, port: port))
                } catch {
                    continuation.resume(throwing: HTTP3ProxyError.invalidConfiguration)
                }
            }
        }
    }

    func authorizeBind(_ address: SocketAddress, host: String) throws {
        guard !inputs.configuration.allowNonLoopbackBind else { return }
        let loopback: Bool
        switch address {
        case .v4: loopback = address.ipAddress?.split(separator: ".").first == "127"
        case .v6: loopback = address.ipAddress == "::1"
        case .unixDomainSocket: loopback = false
        }
        guard loopback else { throw ProxyServerError.nonLoopbackBindRejected(host) }
    }

    func bind(
        state: NIOLoopBound<HTTP3Transport>,
        loop: any EventLoop,
        authenticator: Authenticator,
        address: SocketAddress
    ) async throws -> Int {
        do {
            let ipv4Verifier = try makeVerifier(on: loop)
            _ = try await loop.flatSubmit {
                HTTP3Transport.startClient(state: state, loop: loop, verifier: ipv4Verifier, ipv6: false)
            }
            .get()
            try Task.checkCancellation()
            let ipv6Verifier = try makeVerifier(on: loop)
            _ = try await loop.flatSubmit {
                HTTP3Transport.startClient(state: state, loop: loop, verifier: ipv6Verifier, ipv6: true)
            }
            .get()
            try Task.checkCancellation()
            return try await loop.flatSubmit {
                HTTP3Transport.startServer(state: state, loop: loop, authenticator: authenticator, address: address)
                    .flatMapThrowing { channel in
                        guard let port = channel.localAddress?.port else { throw HTTP3ProxyError.invalidConfiguration }
                        return port
                    }
            }
            .get()
        } catch is CancellationError {
            throw CancellationError()
        } catch { throw HTTP3ProxyError.invalidConfiguration }
    }
}

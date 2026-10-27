import Foundation
import NIOCore
import NIOHTTP3
import NIOQUIC
import SwiftASN1
import SwiftMITM
import X509

public enum HTTP3ProxyError: Error, Equatable, Sendable {
    case invalidConfiguration
    case invalidRoute
    case egressDenied
    case destinationUnavailable
    case upstreamConnectionFailed
    case tlsHandshakeFailed
    case protocolViolation
    case unsupportedProtocol
    case resourceLimit
    case handshakeTimedOut
    case shutdownTimedOut
}

public struct HTTP3FixedRoute: Sendable {
    public let authority: String
    public let destination: CapturedNetworkEndpoint
    let identity: HTTPAuthority
    let address: SocketAddress

    public init(authority: String, destination: CapturedNetworkEndpoint) throws {
        guard let identity = HTTPAuthority(authority: authority), (1...65535).contains(destination.port) else {
            throw HTTP3ProxyError.invalidRoute
        }
        do { address = try SocketAddress(ipAddress: destination.address, port: destination.port) } catch {
            throw HTTP3ProxyError.invalidRoute
        }
        self.authority = identity.authority
        self.destination = destination
        self.identity = identity
    }
}

public enum HTTP3TrustRoots: Sendable {
    case system
    case customPEM([String])
}

public struct HTTP3ProxyConfiguration: Sendable {
    public let route: HTTP3FixedRoute
    public let limits: HTTP3ResourceLimits
    public let upstreamTrust: HTTP3TrustRoots
    public let egressPolicy: EgressPolicy
    public let captureBodyLimit: Int
    public let allowNonLoopbackBind: Bool
    let certificates: [Certificate]?
    let admissionConfiguration: ResourceAdmissionConfiguration

    public init(
        route: HTTP3FixedRoute,
        limits: HTTP3ResourceLimits = .default,
        upstreamTrust: HTTP3TrustRoots = .system,
        egressPolicy: EgressPolicy = .default,
        captureBodyLimit: Int = 0,
        allowNonLoopbackBind: Bool = false
    ) throws {
        guard captureBodyLimit >= 0, captureBodyLimit <= limits.maximumRetainedBytes else {
            throw HTTP3ProxyError.invalidConfiguration
        }
        let nativeBytes = try QUICStreamChannels.requiredStorageBytes(
            maximumBytesPerRead: 16_384,
            maximumPendingWriteBytes: 65_536
        )
        // Preflight only queries the native size model; allocation is forbidden until server admission exists.
        let parserBytes = HTTP3InboundMemoryConfiguration(
            maximumEncodedFieldSectionBytes: 32_768,
            maximumBytesPerRead: 16_384,
            makeStorageReservation: { _ in throw HTTP3ProxyError.invalidConfiguration }
        ).requiredStorageBytes(maximumDecodedFieldSectionBytes: 32_768)
        guard limits.maximumRetainedBytes >= max(nativeBytes, parserBytes) else {
            throw HTTP3ProxyError.invalidConfiguration
        }
        certificates = try Self.parseTrust(upstreamTrust)
        admissionConfiguration = try ResourceAdmissionConfiguration(
            maximumConnections: limits.maximumConnections,
            maximumStreams: 16_384,
            maximumConnectionsPerDestination: limits.maximumConnections,
            maximumRetainedBytes: limits.maximumRetainedBytes
        )
        self.route = route
        self.limits = limits
        self.upstreamTrust = upstreamTrust
        self.egressPolicy = egressPolicy
        self.captureBodyLimit = captureBodyLimit
        self.allowNonLoopbackBind = allowNonLoopbackBind
    }

    private static func parseTrust(_ roots: HTTP3TrustRoots) throws -> [Certificate]? {
        guard case let .customPEM(values) = roots else { return nil }
        guard !values.isEmpty else { throw HTTP3ProxyError.invalidConfiguration }
        do {
            let certificates = try values.flatMap { value in
                try PEMDocument.parseMultiple(pemString: value).map { try Certificate(pemDocument: $0) }
            }
            guard !certificates.isEmpty else { throw HTTP3ProxyError.invalidConfiguration }
            return certificates
        } catch {
            throw HTTP3ProxyError.invalidConfiguration
        }
    }
}

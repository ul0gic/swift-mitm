import Foundation
import SwiftMITM
import SwiftMITMHTTP3

enum HostError: Error {
    case arguments
    case captureOverflow
    case unexpectedControlSession
}

struct HostConfiguration: Decodable, Sendable {
    let authority: String
    let destinationAddress: String
    let destinationPort: Int
    let originCAPath: String
    let readyPath: String
    let allowInternal: Bool
    let maximumConnections: Int
    let maximumPendingHandshakes: Int
    let maximumRequestStreamsPerConnection: Int
    let maximumRetainedBytes: Int
    let controlPreset: String?
    let mappedAuthority: String?
    let sharedRetainedBytes: Int?
    let callerOwnedGroup: Bool?
    let handshakeDeadlineSeconds: Int?
    let observe: Bool?

    func configuration() throws -> HTTP3ProxyConfiguration {
        let route = try HTTP3FixedRoute(
            authority: authority,
            destination: CapturedNetworkEndpoint(address: destinationAddress, port: destinationPort)
        )
        let limits = try HTTP3ResourceLimits(
            maximumConnections: maximumConnections,
            maximumPendingHandshakes: maximumPendingHandshakes,
            maximumRequestStreamsPerConnection: maximumRequestStreamsPerConnection,
            maximumRetainedBytes: maximumRetainedBytes,
            handshakeDeadline: .seconds(handshakeDeadlineSeconds ?? 10)
        )
        let root = try String(contentsOfFile: originCAPath, encoding: .utf8)
        return try HTTP3ProxyConfiguration(
            route: route,
            limits: limits,
            upstreamTrust: .customPEM([root]),
            egressPolicy: EgressPolicy(allowInternal: allowInternal),
            captureBodyLimit: 0
        )
    }
}

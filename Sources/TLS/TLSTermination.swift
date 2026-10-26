import NIOSSL

enum ALPNProtocol: String, Sendable {
    case http2 = "h2"
    case http11 = "http/1.1"
}

struct TLSTermination: Sendable {
    let identities: LeafIdentityCache
    let defaultHost: String

    func makeServerConfiguration(
        baseIdentity: MintedIdentity,
        applicationProtocols: [ALPNProtocol] = [.http2, .http11]
    ) -> TLSConfiguration {
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: baseIdentity.certificateChain,
            privateKey: baseIdentity.privateKey
        )
        configuration.applicationProtocols = applicationProtocols.map(\.rawValue)

        let identities = identities
        let defaultHost = defaultHost
        configuration.sslContextCallback = { values, promise in
            guard let host = values.serverHostname, host.lowercased() != defaultHost.lowercased() else {
                promise.succeed(.noChanges)
                return
            }
            identities.identity(forHost: host, on: promise.futureResult.eventLoop)
                .map { identity in
                var override = NIOSSLContextConfigurationOverride()
                    override.certificateChain = identity.certificateChain
                    override.privateKey = identity.privateKey
                    return override
                }
                .cascade(to: promise)
        }
        return configuration
    }
}

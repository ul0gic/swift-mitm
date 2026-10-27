import NIOSSL

enum ALPNProtocol: String, Sendable {
    case http2 = "h2"
    case http11 = "http/1.1"
}

struct TLSTermination: Sendable {
    func makeServerConfiguration(
        baseIdentity: MintedIdentity,
        applicationProtocols: [ALPNProtocol] = [.http2, .http11]
    ) -> TLSConfiguration {
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: baseIdentity.certificateChain,
            privateKey: baseIdentity.privateKey
        )
        configuration.applicationProtocols = applicationProtocols.map(\.rawValue)

        return configuration
    }
}

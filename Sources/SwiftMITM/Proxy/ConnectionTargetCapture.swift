import NIOCore

extension ConnectionTarget {
    func capturedTarget(destination: SocketAddress? = nil) -> CapturedTarget {
        let destinationAddress = destination?.ipAddress ?? connectionHost
        let destinationPort = destination?.port ?? port
        let originalClient: CapturedNetworkEndpoint?
        let provenance: CapturedIngressProvenance
        switch ingressProvenance {
        case .explicitConnect:
            provenance = .explicitConnect
            originalClient = nil
        case .trustedProxyV2(let address):
            provenance = .trustedProxyV2
            originalClient = address.ipAddress.map {
                CapturedNetworkEndpoint(address: $0, port: address.port ?? 0)
            }
        }
        return CapturedTarget(
            destination: CapturedNetworkEndpoint(address: destinationAddress, port: destinationPort),
            logicalAuthority: logicalAuthority,
            tlsServerName: tlsServerName,
            ingressProvenance: provenance,
            originalClient: originalClient
        )
    }
}

extension ResolvedTarget {
    var capturedTarget: CapturedTarget {
        unresolved.capturedTarget(destination: connectedAddress)
    }
}

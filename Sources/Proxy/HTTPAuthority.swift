public struct HTTPAuthority: Sendable {
    public let host: String
    public let port: Int
    public let authority: String

    public init?(authority: String) {
        guard let target = ConnectionTarget(explicitConnectAuthority: authority) else { return nil }
        host = target.connectionHost
        port = target.port
        self.authority = target.logicalAuthority
    }
}

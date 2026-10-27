public struct HTTP3ResourceLimits: Sendable {
    public let maximumConnections: Int
    public let maximumPendingHandshakes: Int
    public let maximumRequestStreamsPerConnection: Int
    public let maximumRetainedBytes: Int
    public let handshakeDeadline: Duration

    public static let `default` = Self(defaults: ())

    public init(
        maximumConnections: Int = 128,
        maximumPendingHandshakes: Int = 64,
        maximumRequestStreamsPerConnection: Int = 128,
        maximumRetainedBytes: Int = 64 * 1024 * 1024,
        handshakeDeadline: Duration = .seconds(10)
    ) throws {
        guard (1...1024).contains(maximumConnections),
              (1...256).contains(maximumPendingHandshakes),
              (1...1024).contains(maximumRequestStreamsPerConnection),
              (1...268_435_456).contains(maximumRetainedBytes),
              handshakeDeadline >= .seconds(1), handshakeDeadline <= .seconds(30) else {
            throw HTTP3ProxyError.invalidConfiguration
        }
        self.maximumConnections = maximumConnections
        self.maximumPendingHandshakes = maximumPendingHandshakes
        self.maximumRequestStreamsPerConnection = maximumRequestStreamsPerConnection
        self.maximumRetainedBytes = maximumRetainedBytes
        self.handshakeDeadline = handshakeDeadline
    }

    private init(defaults: Void) {
        maximumConnections = 128
        maximumPendingHandshakes = 64
        maximumRequestStreamsPerConnection = 128
        maximumRetainedBytes = 64 * 1024 * 1024
        handshakeDeadline = .seconds(10)
    }
}

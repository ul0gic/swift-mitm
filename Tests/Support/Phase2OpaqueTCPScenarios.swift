struct Phase2OpaqueTCPScenario: Sendable {
    static let maximumPayloadBytes = 64 * 1_024

    let name: String
    let clientInitialBytes: [UInt8]
    let serverInitialBytes: [UInt8]
    let serverReplyBytes: [UInt8]
}

enum Phase2OpaqueTCPScenarios {
    static let clientFirst = Phase2OpaqueTCPScenario(
        name: "client-first",
        clientInitialBytes: [0x43, 0x31],
        serverInitialBytes: [],
        serverReplyBytes: [0x53, 0x31]
    )

    static let serverFirst = Phase2OpaqueTCPScenario(
        name: "server-first",
        clientInitialBytes: [0x43, 0x32],
        serverInitialBytes: [0x53, 0x32],
        serverReplyBytes: []
    )

    static let bidirectional = Phase2OpaqueTCPScenario(
        name: "bidirectional",
        clientInitialBytes: [0x43, 0x33, 0x43, 0x34],
        serverInitialBytes: [0x53, 0x33],
        serverReplyBytes: [0x53, 0x34]
    )

    static let clientHalfClose = Phase2OpaqueTCPScenario(
        name: "client-half-close",
        clientInitialBytes: [0x43, 0x35],
        serverInitialBytes: [],
        serverReplyBytes: [0x53, 0x35]
    )

    static let serverHalfClose = Phase2OpaqueTCPScenario(
        name: "server-half-close",
        clientInitialBytes: [0x43, 0x36],
        serverInitialBytes: [0x53, 0x36],
        serverReplyBytes: []
    )

    static let stalledReader = Phase2OpaqueTCPScenario(
        name: "stalled-reader",
        clientInitialBytes: [],
        serverInitialBytes: Array(repeating: 0x5A, count: Phase2OpaqueTCPScenario.maximumPayloadBytes),
        serverReplyBytes: []
    )
}

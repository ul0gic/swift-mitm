enum Phase2OpaqueTCPEvent: Equatable, Sendable {
    case clientBytes([UInt8])
    case serverBytes([UInt8])
    case clientOutputClosed
    case serverOutputClosed
    case clientReadsPaused
    case clientReadsResumed
}

struct Phase2OpaqueTCPScenario: Sendable {
    static let maximumPayloadBytes = 64 * 1_024

    let name: String
    let clientInitialBytes: [UInt8]
    let serverInitialBytes: [UInt8]
    let serverReplyBytes: [UInt8]
    let expectedEvents: [Phase2OpaqueTCPEvent]
}

enum Phase2OpaqueTCPScenarios {
    static let clientFirst = Phase2OpaqueTCPScenario(
        name: "client-first",
        clientInitialBytes: [0x43, 0x31],
        serverInitialBytes: [],
        serverReplyBytes: [0x53, 0x31],
        expectedEvents: [.clientBytes([0x43, 0x31]), .serverBytes([0x53, 0x31])]
    )

    static let serverFirst = Phase2OpaqueTCPScenario(
        name: "server-first",
        clientInitialBytes: [0x43, 0x32],
        serverInitialBytes: [0x53, 0x32],
        serverReplyBytes: [],
        expectedEvents: [.serverBytes([0x53, 0x32]), .clientBytes([0x43, 0x32])]
    )

    static let bidirectional = Phase2OpaqueTCPScenario(
        name: "bidirectional",
        clientInitialBytes: [0x43, 0x33, 0x43, 0x34],
        serverInitialBytes: [0x53, 0x33],
        serverReplyBytes: [0x53, 0x34],
        expectedEvents: [
            .serverBytes([0x53, 0x33]),
            .clientBytes([0x43, 0x33, 0x43, 0x34]),
            .serverBytes([0x53, 0x34])
        ]
    )

    static let clientHalfClose = Phase2OpaqueTCPScenario(
        name: "client-half-close",
        clientInitialBytes: [0x43, 0x35],
        serverInitialBytes: [],
        serverReplyBytes: [0x53, 0x35],
        expectedEvents: [
            .clientBytes([0x43, 0x35]),
            .clientOutputClosed,
            .serverBytes([0x53, 0x35])
        ]
    )

    static let serverHalfClose = Phase2OpaqueTCPScenario(
        name: "server-half-close",
        clientInitialBytes: [0x43, 0x36],
        serverInitialBytes: [0x53, 0x36],
        serverReplyBytes: [],
        expectedEvents: [
            .serverBytes([0x53, 0x36]),
            .serverOutputClosed,
            .clientBytes([0x43, 0x36])
        ]
    )

    static let stalledReader = Phase2OpaqueTCPScenario(
        name: "stalled-reader",
        clientInitialBytes: [],
        serverInitialBytes: Array(repeating: 0x5A, count: Phase2OpaqueTCPScenario.maximumPayloadBytes),
        serverReplyBytes: [],
        expectedEvents: [
            .clientReadsPaused,
            .serverBytes(Array(repeating: 0x5A, count: Phase2OpaqueTCPScenario.maximumPayloadBytes)),
            .clientReadsResumed
        ]
    )

    static let all = [clientFirst, serverFirst, bidirectional, clientHalfClose, serverHalfClose, stalledReader]
}

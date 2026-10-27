enum Phase2FixtureError: Error, Equatable {
    case closedBeforeExpectedBytes
    case deadlineExceeded
    case exceededByteLimit
    case unexpectedBytes
}

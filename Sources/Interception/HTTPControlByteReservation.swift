import NIOConcurrencyHelpers

final class HTTPControlBudget: Sendable {
    struct State {
        var configuration: HTTPControlConfiguration
        var bytes = 0
        var reservations = 0
        var stopped = false
    }
    let state: NIOLockedValueBox<State>

    init(configuration: HTTPControlConfiguration) { state = NIOLockedValueBox(State(configuration: configuration)) }

    func charge(_ bytes: Int) throws {
        try state.withLockedValue { value in
            guard bytes >= 0,
                  bytes <= value.configuration.limits.maximumRetainedBytes - value.bytes
                  else { throw HTTPControlError.capacityExceeded }
            value.bytes += bytes
        }
    }

    func release(_ bytes: Int) { state.withLockedValue { $0.bytes -= bytes } }
}

public final class HTTPControlByteReservation: Sendable {
    private let releaseAction: NIOLockedValueBox<(@Sendable () -> Void)?>

    init(release: @escaping @Sendable () -> Void) { releaseAction = NIOLockedValueBox(release) }
    public func release() {
        let action = releaseAction.withLockedValue { action in
            let current = action
            action = nil
            return current
        }
        action?()
    }
    deinit { release() }
}

import NIOConcurrencyHelpers
import NIOCore

struct ReplayMeasurement: Sendable {
    let timings: HARExchangeTiming?
    let unavailable: ReplayTimingUnavailable?

    static let synthetic = ReplayMeasurement(timings: nil, unavailable: .syntheticResponse)
}

final class ReplayTimingRecorder: Sendable {
    private struct State {
        var sendStart: UInt64?
        var sendEnd: UInt64?
        var responseStart: UInt64?
        var responseEnd: UInt64?
    }

    private let state = NIOLockedValueBox(State())

    func startSend() { state.withLockedValue { $0.sendStart = NIODeadline.now().uptimeNanoseconds } }
    func finishSend() { state.withLockedValue { $0.sendEnd = NIODeadline.now().uptimeNanoseconds } }

    func startResponse() {
        state.withLockedValue {
            if $0.responseStart == nil {
                $0.responseStart = NIODeadline.now().uptimeNanoseconds
            }
        }
    }

    func finishResponse() { state.withLockedValue { $0.responseEnd = NIODeadline.now().uptimeNanoseconds } }

    func measurement() throws -> ReplayMeasurement {
        try state.withLockedValue { state in
            guard let sendStart = state.sendStart, let sendEnd = state.sendEnd else {
                return ReplayMeasurement(timings: nil, unavailable: .incompleteUpload)
            }
            guard let responseStart = state.responseStart, let responseEnd = state.responseEnd,
                sendStart <= sendEnd, sendEnd <= responseStart, responseStart <= responseEnd
            else { return ReplayMeasurement(timings: nil, unavailable: .overlappingExchange) }
            return try ReplayMeasurement(
                timings: HARExchangeTiming(
                    send: Double(sendEnd - sendStart) / 1_000_000,
                    wait: Double(responseStart - sendEnd) / 1_000_000,
                    receive: Double(responseEnd - responseStart) / 1_000_000
                ),
                unavailable: nil
            )
        }
    }
}

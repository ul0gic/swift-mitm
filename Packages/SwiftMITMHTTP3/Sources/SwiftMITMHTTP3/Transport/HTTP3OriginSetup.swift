import Foundation
import NIOCore

final class HTTP3OriginSetup {
    let requestID: UUID
    let promise: EventLoopPromise<HTTP3OriginStream>
    var initialize: ((any Channel) -> EventLoopFuture<Void>)?
    weak var waitingSession: HTTP3OriginSession?
    private(set) var completed = false

    init(
        requestID: UUID,
        eventLoop: any EventLoop,
        initialize: @escaping (any Channel) -> EventLoopFuture<Void>
    ) {
        self.requestID = requestID
        self.initialize = initialize
        promise = eventLoop.makePromise(of: HTTP3OriginStream.self)
    }

    func fail(_ error: any Error) {
        guard !completed else { return }
        completed = true
        initialize = nil
        detachWaiter()
        promise.fail(error)
    }

    func succeed(_ stream: HTTP3OriginStream) {
        guard !completed else {
            stream.channel.close(promise: nil)
            return
        }
        completed = true
        initialize = nil
        detachWaiter()
        promise.succeed(stream)
    }

    private func detachWaiter() {
        waitingSession?.waiters.removeValue(forKey: requestID)
        waitingSession = nil
    }
}

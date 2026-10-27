import Foundation
import NIOCore
import NIOHTTP3
import NIOQUIC

final class ProbeConnectionObserver: ChannelInboundHandler {
    typealias InboundIn = Never

    private let receivedGoaway: () -> Void

    init(receivedGoaway: @escaping () -> Void) {
        self.receivedGoaway = receivedGoaway
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is ReceivedGoawayEvent {
            receivedGoaway()
        }
        context.fireUserInboundEventTriggered(event)
    }
}

extension ProbeTransport {
    func trackConnectionStorage(_ channel: Channel) throws {
        guard let options = channel.syncOptions else { throw ProbeError.missingHandler }
        let future = try options.getOption(QUICConnectionStreamStorageReleasedFutureOption())
        let id = UUID()
        connectionStorage[id] = future
        metrics.connectionStorageStarted()
        let state = NIOLoopBound(self, eventLoop: channel.eventLoop)
        future.whenSuccess {
            state.value.connectionStorage.removeValue(forKey: id)
            state.value.metrics.connectionStorageReleased()
        }
    }
}

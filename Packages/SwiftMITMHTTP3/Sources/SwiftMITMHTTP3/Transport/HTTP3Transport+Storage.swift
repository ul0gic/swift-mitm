import Foundation
import NIOCore
import NIOQUIC

extension HTTP3Transport {
    func trackConnection(_ channel: any Channel, ownership: HTTP3ConnectionOwnership?) throws {
        guard let options = channel.syncOptions else { throw HTTP3ProxyError.protocolViolation }
        let released = try options.getOption(QUICConnectionStreamStorageReleasedFutureOption())
        let id = UUID()
        connectionStorage[id] = released
        resources.storageStarted()
        ownership?.attached = true
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let owner = NIOLoopBound(ownership, eventLoop: eventLoop)
        released.whenSuccess {
            owner.value?.release()
            state.value.connectionStorage.removeValue(forKey: id)
            state.value.resources.storageReleased()
            state.value.checkQuiescenceLater()
        }
    }

    static func receiveStream(_ channel: any Channel, state: NIOLoopBound<HTTP3Transport>) -> EventLoopFuture<Void> {
        channel.eventLoop.flatSubmit {
            do {
                try state.value.resources.track(channel)
                try channel.pipeline.syncOperations.addHandler(
                    HTTP3RawReadMeter(resources: state.value.resources),
                    position: .first
                )
                guard let parent = channel.parent else { throw HTTP3ProxyError.protocolViolation }
                let handler = try parent.pipeline.syncOperations.handler(type: HTTP3NativeHandler.self)
                return handler.inboundStreamReceived(channel)
            } catch { return channel.eventLoop.makeFailedFuture(error) }
        }
    }

    func sampleResources() -> HTTP3ResourceUsage {
        guard !disposalComplete else { return resources.snapshot() }
        if let serverHandler {
            resources.observeListener(serverHandler.connectionAdmissionSnapshot)
        }
        resources.sampleNativeOutput()
        return resources.snapshot()
    }

    func completeListener() {
        guard !listenerCompleted else { return }
        if let serverHandler {
            resources.observeListener(serverHandler.connectionAdmissionSnapshot)
        }
        resources.completeListenerIncarnation()
        listenerCompleted = true
        serverHandler = nil
    }
}

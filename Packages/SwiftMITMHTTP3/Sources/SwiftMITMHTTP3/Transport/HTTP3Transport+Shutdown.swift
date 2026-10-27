import NIOCore

extension HTTP3Transport {
    func stop() -> EventLoopFuture<Void> {
        if let stopPromise {
            return stopPromise.futureResult
        }
        let promise = eventLoop.makePromise(of: Void.self)
        stopPromise = promise
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let deadline = NIODeadline.now() + .seconds(2)
        stopDeadline = eventLoop.scheduleTask(deadline: deadline) {
            let transport = state.value
            transport.closeSockets()
            transport.checkQuiescenceLater()
            transport.eventLoop.execute {
                guard let pending = state.value.stopPromise else { return }
                state.value.stopPromise = nil
                state.value.stopDeadline = nil
                pending.fail(HTTP3ProxyError.shutdownTimedOut)
            }
        }
        if closing {
            checkQuiescenceLater()
            return promise.futureResult
        }
        closing = true
        cancelExchanges(matching: { _ in true }, reason: .cancelled)
        for session in sessions.values { session.draining = true }
        let handlers = [serverHandler, clientHandler, clientHandlerIPv6].compactMap(\.self)
        EventLoopFuture.andAllComplete(
            handlers.map { $0.shutdownGracefully(deadline: deadline) },
            on: eventLoop
        )
        .whenComplete { _ in state.value.closeSockets() }
        return promise.futureResult
    }

    private func closeSockets() {
        for downstream in downstreams.values { downstream.channel.close(promise: nil) }
        for session in sessions.values { session.channel?.close(promise: nil) }
        let sockets = [serverSocket, clientSocket, clientSocketIPv6].compactMap(\.self)
        for socket in sockets where socket.isActive { socket.close(promise: nil) }
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        EventLoopFuture.andAllSucceed(sockets.map(\.closeFuture), on: eventLoop).whenComplete { result in
            switch result {
            case .success:
                state.value.socketsClosed = true
                state.value.checkQuiescenceLater()
            case .failure:
                state.value.stopPromise?.fail(HTTP3ProxyError.shutdownTimedOut)
                state.value.stopPromise = nil
            }
        }
    }

    func checkQuiescenceLater() {
        guard closing else { return }
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        eventLoop.execute { state.value.checkQuiescence() }
    }

    private func checkQuiescence() {
        guard socketsClosed, pendingResolutions == 0, connectionStorage.isEmpty,
              exchanges.isEmpty, resources.storageIsQuiescent else { return }
        completeListener()
        resources.finalizeNativeOccupancy()
        disposalComplete = true
        sessions.removeAll()
        fixedSessions.removeAll()
        downstreams.removeAll()
        serverSocket = nil
        clientSocket = nil
        clientSocketIPv6 = nil
        clientHandler = nil
        clientHandlerIPv6 = nil
        stopDeadline?.cancel()
        stopDeadline = nil
        let promise = stopPromise
        stopPromise = nil
        promise?.succeed()
    }
}

import Foundation
import NIOCore
import SwiftMITM

extension HTTP3Transport {
    func openOriginStream(
        requestID: UUID,
        selection: HTTP3OriginSelection,
        initialize: @escaping (any Channel) -> EventLoopFuture<Void>
    ) -> EventLoopFuture<HTTP3OriginStream> {
        let setup = HTTP3OriginSetup(requestID: requestID, eventLoop: eventLoop, initialize: initialize)
        guard let exchange = exchanges[requestID], !closing, !exchange.setupCancelled else {
            setup.fail(CancellationError())
            return setup.promise.futureResult
        }
        guard !exchange.setupStarted else {
            setup.fail(HTTP3ProxyError.protocolViolation)
            return setup.promise.futureResult
        }
        exchange.setupStarted = true
        exchange.setup = setup
        do {
            switch selection {
            case .fixed:
                try openFixedOrigin(exchange: exchange, setup: setup)
            case let .mapped(authority):
                if let literal = try? SocketAddress(ipAddress: authority.host, port: authority.port) {
                    try authorize(literal)
                }
                let ownership = try HTTP3ConnectionOwnership(
                    resources: resources,
                    configuration: configuration,
                    destination: authority.authority
                )
                resolve(authority, setup: setup, ownership: ownership)
            }
        } catch {
            let failure = error as? HTTP3ProxyError ?? .upstreamConnectionFailed
            emitFailure(id: requestID, target: exchange.downstream.target, error: failure)
            setup.fail(failure)
        }
        return setup.promise.futureResult
    }

    func authorize(_ address: SocketAddress) throws {
        guard !configuration.egressPolicy.denies(address) else { throw HTTP3ProxyError.egressDenied }
    }

    private func openFixedOrigin(exchange: HTTP3ExchangeRegistration, setup: HTTP3OriginSetup) throws {
        if let id = fixedSessions[exchange.downstream.id], let session = sessions[id], !session.draining {
            attach(setup, to: session)
            return
        }
        let route = configuration.route
        try authorize(route.address)
        let ownership = try HTTP3ConnectionOwnership(
            resources: resources,
            configuration: configuration,
            destination: route.authority
        )
        let session = try makeSession(
            exchange: exchange,
            authority: route.identity,
            address: route.address,
            exclusiveRequestID: nil,
            ownership: ownership
        )
        attach(setup, to: session)
        connect(session)
    }

    func makeSession(
        exchange: HTTP3ExchangeRegistration,
        authority: HTTPAuthority,
        address: SocketAddress,
        exclusiveRequestID: UUID?,
        ownership: HTTP3ConnectionOwnership
    ) throws -> HTTP3OriginSession {
        guard let numericHost = address.ipAddress, let numericPort = address.port else {
            ownership.release()
            throw HTTP3ProxyError.invalidRoute
        }
        let target = CapturedTarget(
            destination: CapturedNetworkEndpoint(address: numericHost, port: numericPort),
            logicalAuthority: authority.authority,
            tlsServerName: Self.wireServerName(authority),
            ingressProvenance: .fixedRouteHTTP3,
            originalClient: exchange.downstream.target.originalClient
        )
        let session = HTTP3OriginSession(
            downstreamID: exchange.downstream.id,
            exclusiveRequestID: exclusiveRequestID,
            target: target,
            route: (authority, address),
            ownership: ownership,
            eventLoop: eventLoop
        )
        sessions[session.id] = session
        if exclusiveRequestID == nil {
            fixedSessions[session.downstreamID] = session.id
        }
        observeOriginConnection(session)
        return session
    }

    static func wireServerName(_ authority: HTTPAuthority) -> String? {
        (try? SocketAddress(ipAddress: authority.host, port: authority.port)) == nil ? authority.host : nil
    }

    private func attach(_ setup: HTTP3OriginSetup, to session: HTTP3OriginSession) {
        guard !setup.completed, !session.draining, let exchange = exchanges[setup.requestID],
              !exchange.setupCancelled else {
            setup.fail(CancellationError())
            return
        }
        exchange.sessionID = session.id
        if let result = session.connectionResult {
            completeOriginWaiter(setup, session: session, result: result)
        } else {
            session.waiters[setup.requestID] = setup
            setup.waitingSession = session
        }
    }

    private func observeOriginConnection(_ session: HTTP3OriginSession) {
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let owned = NIOLoopBound(session, eventLoop: eventLoop)
        session.promise.futureResult.whenComplete { result in
            let session = owned.value
            session.connectionResult = result
            while let requestID = session.waiters.keys.first,
                  let setup = session.waiters.removeValue(forKey: requestID) {
                setup.waitingSession = nil
                state.value.completeOriginWaiter(setup, session: session, result: result)
            }
        }
    }

    private func completeOriginWaiter(
        _ setup: HTTP3OriginSetup,
        session: HTTP3OriginSession,
        result: Result<any Channel, any Error>
    ) {
        switch result {
        case let .success(channel): openRequestStream(setup, session: session, connection: channel)
        case let .failure(error): setup.fail(error)
        }
    }

    func resolve(_ authority: HTTPAuthority, setup: HTTP3OriginSetup, ownership: HTTP3ConnectionOwnership) {
        let state = NIOLoopBound(self, eventLoop: eventLoop)
        let owned = NIOLoopBound((setup, ownership), eventLoop: eventLoop)
        ownership.timedOut = {
            guard !owned.value.0.completed else { return }
            state.value.emitFailure(
                id: setup.requestID,
                target: state.value.exchanges[setup.requestID]?.downstream.target,
                error: .handshakeTimedOut
            )
            owned.value.0.fail(HTTP3ProxyError.handshakeTimedOut)
        }
        ownership.startDeadline(on: eventLoop, duration: configuration.limits.handshakeDeadline)
        pendingResolutions += 1
        let loop = eventLoop
        resolutionQueue.async {
            let result = Result { try SocketAddress.makeAddressResolvingHost(authority.host, port: authority.port) }
            loop.execute {
                state.value.resolved(result, authority: authority, setup: owned.value.0, ownership: owned.value.1)
            }
        }
    }

    private func resolved(
        _ result: Result<SocketAddress, any Error>,
        authority: HTTPAuthority,
        setup: HTTP3OriginSetup,
        ownership: HTTP3ConnectionOwnership
    ) {
        pendingResolutions -= 1
        defer { checkQuiescenceLater() }
        guard !closing, !setup.completed, let exchange = exchanges[setup.requestID], !exchange.setupCancelled else {
            ownership.release()
            setup.fail(CancellationError())
            return
        }
        do {
            let address: SocketAddress
            do { address = try result.get() } catch { throw HTTP3ProxyError.destinationUnavailable }
            try authorize(address)
            let session = try makeSession(
                exchange: exchange,
                authority: authority,
                address: address,
                exclusiveRequestID: setup.requestID,
                ownership: ownership
            )
            attach(setup, to: session)
            connect(session)
        } catch {
            ownership.release()
            let failure = error as? HTTP3ProxyError ?? .destinationUnavailable
            emitFailure(id: setup.requestID, target: exchange.downstream.target, error: failure)
            setup.fail(failure)
        }
    }
}

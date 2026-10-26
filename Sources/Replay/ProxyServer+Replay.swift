import Foundation
import NIOCore
import NIOHTTP1
import SwiftMITMHTTP1Codec

extension ProxyServer {
    public func replay(
        _ request: ReplayRequest, responseSink: any ReplayResponseSink
    ) async throws -> ReplayResult {
        let identity = ReplayIdentity(requestID: UUID(), attemptID: UUID(), source: request.source)
        do {
            return try await performReplay(request, identity: identity, responseSink: responseSink)
        } catch let error as HTTPControlError {
            controlEventSink?.receive(.failure(requestID: identity.requestID, code: error))
            throw error
        }
    }

    private func performReplay(
        _ request: ReplayRequest, identity: ReplayIdentity, responseSink: any ReplayResponseSink
    ) async throws -> ReplayResult {
        guard request.bodyIsComplete else { throw ReplayError.incompleteBody }
        guard request.body.count <= replayCoordinator.configuration.maximumBodyBytes else {
            throw ReplayError.bodyLimit
        }
        let loop = try replayEventLoop()
        let original = try ReplayPreparedRequest(request)
        let attempt = try replayCoordinator.acquire()
        defer { attempt.release() }
        let admissionAttempt = try admission?.acquire(.replay)
        defer { admissionAttempt?.release() }
        let bodyReservation = try admission?.acquire(.retainedBytes(request.body.count + 262_144))
        defer { bodyReservation?.release() }
        try await responseSink.receive(.started(
            attemptID: identity.attemptID, requestID: identity.requestID,
            sourceRequestID: identity.source.requestID, sourceRepresentation: identity.source.representation
        ))
        try Task.checkCancellation()
        let controls = try replayControls(request, target: original.target, requestID: identity.requestID)
        let decision = try await controls?.requestDecision()
        let replacementReservation = try replayReplacementReservation(decision)
        defer { replacementReservation?.release() }
        let effective = try original.applying(decision, to: request)
        guard effective.body.count <= replayCoordinator.configuration.maximumBodyBytes else {
            throw ReplayError.bodyLimit
        }
        let invocation = ReplayInvocation(
            source: request, effective: effective, original: original,
            prepared: try ReplayPreparedRequest(effective), identity: identity,
            controls: controls, faults: decision?.faults ?? [], responseSink: responseSink
        )
        if let decision, case .respond(let status, _, let body) = decision.action {
            guard let headers = decision.syntheticResponseHeaders else { throw HTTPControlError.invalidHeaders }
            return try await invocation.respond(status: status, headers: headers, body: body, server: self)
        }
        return try await connectReplay(invocation, on: loop)
    }

    private func replayControls(
        _ request: ReplayRequest, target: HTTPForwardTarget, requestID: UUID
    ) throws -> ReplayControls? {
        guard request.useControls else { return nil }
        guard let controlRuntime else { throw HTTPControlError.invalidConfiguration }
        return try ReplayControls(
            runtime: controlRuntime, sink: controlEventSink,
            requestID: requestID, request: request, target: target
        )
    }

    private func replayReplacementReservation(
        _ decision: HTTPControlDecision?
    ) throws -> ResourceReservation? {
        guard let decision, case .replaceBody(let body, _, _, _) = decision.action else { return nil }
        guard body.count <= replayCoordinator.configuration.maximumBodyBytes else { throw ReplayError.bodyLimit }
        return try admission?.acquire(.retainedBytes(body.count))
    }

    private func connectReplay(_ invocation: ReplayInvocation, on loop: EventLoop) async throws -> ReplayResult {
        guard let setup = connectionRegistry.beginSetup() else { throw ProxyConnectionError.serverStopping }
        defer { setup.complete() }
        let cancellation = ReplayCancellation(setup: setup)
        return try await withTaskCancellationHandler {
            try Task.checkCancellation()
            let connection = try await connectControlledHTTP(
                target: invocation.prepared.target, on: loop, setupToken: setup
            ).get()
            setup.onCancellation(on: connection.channel.eventLoop) { connection.channel.close(promise: nil) }
            cancellation.attach(connection.channel)
            if invocation.controls != nil {
                controlEventSink?.receive(.connectedTarget(
                    requestID: invocation.identity.requestID, target: connection.target
                ))
            }
            do {
                try Task.checkCancellation()
                let channel = try await configureReplay(connection)
                return try await ReplayExecution(
                    server: self, invocation: invocation, connection: connection, channel: channel
                ).run()
            } catch {
                connection.channel.close(promise: nil)
                throw error
            }
        } onCancel: { cancellation.cancel() }
    }

    private func configureReplay(
        _ connection: HTTPForwardConnection
    ) async throws -> NIOAsyncChannel<HTTPClientResponsePart, HTTPClientRequestPart> {
        let channel = connection.channel
        try await channel.setOption(
            ChannelOptions.recvAllocator, value: FixedSizeRecvByteBufferAllocator(capacity: 65_536)
        ).get()
        try await channel.setOption(ChannelOptions.maxMessagesPerRead, value: 1).get()
        let asyncChannel = try await channel.eventLoop.submit {
            var encoder = BytePreservingHTTPRequestEncoder.Configuration()
            encoder.automaticallySetFramingHeaders = false
            try channel.pipeline.syncOperations.addHandlers([
                BytePreservingHTTPRequestEncoder(configuration: encoder),
                ByteToMessageHandler(BytePreservingHTTPResponseDecoder(
                    leftOverBytesStrategy: .dropBytes, informationalResponseStrategy: .forward
                ))
            ])
            return try NIOAsyncChannel<HTTPClientResponsePart, HTTPClientRequestPart>(
                wrappingChannelSynchronously: channel,
                configuration: .init(backPressureStrategy: .init(lowWatermark: 1, highWatermark: 1))
            )
        }.get()
        if let release = connection.releaseBufferedReads {
            try await release().get()
        }
        return asyncChannel
    }
}

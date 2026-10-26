import Foundation
import NIOCore
import NIOHTTP1

struct ReplayExecution: Sendable {
    let server: ProxyServer
    let invocation: ReplayInvocation
    let connection: HTTPForwardConnection
    let channel: NIOAsyncChannel<HTTPClientResponsePart, HTTPClientRequestPart>
    private let timing = ReplayTimingRecorder()

    func run() async throws -> ReplayResult {
        captureOriginal()
        do {
            return try await channel.executeThenClose { inbound, outbound in
                try await exchange(inbound: inbound, outbound: outbound)
            }
        } catch {
            let failure = CaptureEvent.streamError(
                requestID: invocation.identity.requestID,
                message: "Replay exchange did not complete"
            )
            server.sink.receive(failure)
            if invocation.controls != nil {
                observation?.receiveEffective(failure)
            }
            throw error
        }
    }

    private var observation: ObservationCaptureSink? { server.sink as? ObservationCaptureSink }

    private func exchange(
        inbound: NIOAsyncChannelInboundStream<HTTPClientResponsePart>,
        outbound: NIOAsyncChannelOutboundWriter<HTTPClientRequestPart>
    ) async throws -> ReplayResult {
        try await withThrowingTaskGroup(of: Void.self) { uploads in
            uploads.addTask { try await upload(outbound) }
            var reader = ReplayResponseReader(execution: self, timing: timing)
            for try await part in inbound {
                try Task.checkCancellation()
                if case let .head(head) = part, head.status.code >= 200 {
                    uploads.cancelAll()
                }
                if let result = try await reader.receive(part) {
                    uploads.cancelAll()
                    channel.channel.close(promise: nil)
                    try await uploads.waitForAll()
                    try await invocation.responseSink.receive(.end(result))
                    return result
                }
            }
            throw ReplayError.incompleteResponse
        }
    }

    private func captureOriginal() {
        let requestID = invocation.identity.requestID
        let original = invocation.original
        server.sink.receive(.requestHead(CapturedRequestHead(
            id: requestID,
            timestamp: Date(),
            scheme: original.target.scheme,
            authority: original.target.authority,
            method: original.head.method.rawValue,
            path: original.head.uri,
            version: .http11,
            headers: ExplicitHTTPProxyHandler.fields(original.head.headers),
            target: connection.target
        )))
        var capture = CaptureBodyBuffer(limit: server.captureBodyLimit)
        var offset = 0
        while offset < invocation.source.body.count {
            let end = min(offset + 65_536, invocation.source.body.count)
            let bytes = invocation.source.body[offset..<end]
            observation?.receiveBody(requestID: requestID, direction: .request, bytes: bytes)
            server.sink.receive(.requestBodyChunk(
                requestID: requestID,
                bytes: capture.take(bytes),
                byteCount: bytes.count
            ))
            offset = end
        }
        server.sink.receive(.requestEnd(requestID: requestID, truncated: capture.truncated))
    }

    private func upload(_ outbound: NIOAsyncChannelOutboundWriter<HTTPClientRequestPart>) async throws {
        let requestID = invocation.identity.requestID
        var effectiveStarted = false
        do {
            var pacer = ReplayFaultPacer(invocation.faults, direction: .request)
            try await pacer.beforeHead()
            if invocation.controls != nil {
                captureEffectiveHead()
                effectiveStarted = true
            }
            timing.startSend()
            try await outbound.write(.head(invocation.prepared.head))
            try await uploadBody(outbound, pacer: &pacer)
            try await outbound.write(.end(nil))
            timing.finishSend()
            if effectiveStarted {
                observation?.receiveEffective(.requestEnd(requestID: requestID, truncated: false))
            }
        } catch {
            if Task.isCancelled {
                if effectiveStarted {
                    observation?.receiveEffective(.requestEnd(requestID: requestID, truncated: true))
                }
                return
            }
            channel.channel.close(promise: nil)
            throw error
        }
    }

    private func captureEffectiveHead() {
        let prepared = invocation.prepared
        observation?.receiveEffective(.requestHead(CapturedRequestHead(
            id: invocation.identity.requestID,
            timestamp: Date(),
            scheme: prepared.target.scheme,
            authority: prepared.target.authority,
            method: prepared.head.method.rawValue,
            path: prepared.head.uri,
            version: .http11,
            headers: ExplicitHTTPProxyHandler.fields(prepared.head.headers),
            target: connection.target
        )))
    }

    private func uploadBody(
        _ outbound: NIOAsyncChannelOutboundWriter<HTTPClientRequestPart>,
        pacer: inout ReplayFaultPacer
    ) async throws {
        var offset = 0
        while offset < invocation.effective.body.count {
            try Task.checkCancellation()
            let end = min(offset + 65_536, invocation.effective.body.count)
            let bytes = Array(invocation.effective.body[offset..<end])
            try await pacer.send(bytes) {
                if invocation.controls != nil {
                    observation?.receiveBody(
                        requestID: invocation.identity.requestID,
                        direction: .request,
                        representation: .effective,
                        bytes: $0
                    )
                }
                try await outbound.write(.body(.byteBuffer(ByteBuffer(bytes: $0))))
            }
            offset = end
        }
    }
}

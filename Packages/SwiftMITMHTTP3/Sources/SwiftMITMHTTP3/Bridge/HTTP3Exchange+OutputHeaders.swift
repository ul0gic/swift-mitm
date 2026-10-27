import Foundation
import HTTPTypes
import NIOCore
import SwiftMITM

extension HTTP3Exchange {
    func forwardHead(_ flow: HTTP3Flow) throws {
        guard let head = flow.head else { throw HTTP3ProxyError.protocolViolation }
        guard let decision = flow.decision else {
            try enqueue(head, flow: flow)
            if flow.isRequest { openOrigin(.fixed) }
            return
        }
        let fields = decision.headers
        let lease = try transport.resources.reserve(HTTP3Fields.encodedReservation(fields), kind: .forwarding)
        let encoded = try HTTP3Fields.encode(fields)
        let output: HTTP3Message
        var selection = HTTP3OriginSelection.fixed
        switch head {
        case let .request(.head(original)):
            var request = original
            request.headerFields = encoded
            if case let .route(authority, _) = decision.action {
                guard let mapped = HTTPAuthority(authority: authority) else { throw HTTP3ProxyError.invalidRoute }
                request.authority = mapped.authority
                selection = .mapped(mapped)
            }
            output = .request(.head(request))
        case let .response(.head(original)):
            var response = original
            response.headerFields = encoded
            output = .response(.head(response))
        default: throw HTTP3ProxyError.protocolViolation
        }
        try enqueue(output, lease: lease, flow: flow)
        if flow.isRequest { openOrigin(selection) }
    }

    func effectiveEnd(_ end: HTTP3Message, flow: HTTP3Flow) throws -> HTTP3HeldMessage {
        guard let original = trailers(end), let decision = flow.decision else {
            return HTTP3HeldMessage(message: end, lease: nil)
        }
        let snapshot = try HTTP3HeaderSnapshot(original, resources: transport.resources)
        let filtering = try transport.resources.reserve(
            snapshot.fields.count * MemoryLayout<HTTPHeaderField>.stride, kind: .capture
        )
        return try withExtendedLifetime((snapshot, filtering)) {
            let fields = decision.effectiveTrailers(snapshot.fields)
            let lease = try transport.resources.reserve(HTTP3Fields.encodedReservation(fields), kind: .forwarding)
            let encoded = try HTTP3Fields.encode(fields)
            let message: HTTP3Message = flow.isRequest ? .request(.end(encoded)) : .response(.end(encoded))
            return HTTP3HeldMessage(message: message, lease: lease)
        }
    }

    func openOrigin(_ selection: HTTP3OriginSelection) {
        guard !openingOrigin, origin == nil, !failed else { return }
        openingOrigin = true
        transport.openOriginStream(requestID: id, selection: selection) { channel in
            self.response.source = channel
            do {
                try channel.pipeline.syncOperations.addHandler(HTTP3ResponseHandler(exchange: self))
                return channel.eventLoop.makeSucceededVoidFuture()
            } catch { return channel.eventLoop.makeFailedFuture(error) }
        }.assumeIsolated().whenComplete { result in
            self.openingOrigin = false
            switch result {
            case let .success(origin):
                guard !self.failed, !self.finished else { Self.abort(origin.channel); return }
                self.origin = origin
                self.request.destination = origin.channel
                self.transport.controlEventSink?.receive(.connectedTarget(requestID: self.id, target: origin.target))
                self.pump(self.request)
                self.readIfReady(self.response)
            case let .failure(error): self.fail(error)
            }
        }
    }

    func effectiveOutput(_ message: HTTP3Message, flow: HTTP3Flow) throws {
        if let body = message.body {
            try captureBody(body, flow: flow, representation: .effective)
            return
        }
        switch message {
        case let .request(.head(head)):
            try effectiveHead(message, fields: head.headerFields, flow: flow)
        case let .response(.head(head)):
            try effectiveHead(message, fields: head.headerFields, flow: flow)
        default:
            if let fields = trailers(message) {
                let snapshot = try HTTP3HeaderSnapshot(fields, resources: transport.resources)
                withExtendedLifetime(snapshot) {
                    captureTrailers(snapshot.fields, flow: flow, representation: .effective)
                }
            }
        }
    }

    func effectiveHead(_ message: HTTP3Message, fields: HTTPFields, flow: HTTP3Flow) throws {
        let snapshot = try HTTP3HeaderSnapshot(fields, resources: transport.resources)
        withExtendedLifetime(snapshot) {
            captureHead(message, fields: snapshot.fields, representation: .effective)
            transport.controlEventSink?.receive(.effectiveHead(
                requestID: id, phase: flow.phase, headers: snapshot.fields,
                appliedRuleIDs: flow.decision?.appliedRuleIDs ?? []
            ))
        }
    }
}

import Foundation
import HTTPTypes
import NIOCore
import SwiftMITM

extension HTTP3Exchange {
    func receive(_ message: HTTP3Message, flow: HTTP3Flow) {
        guard !failed, !finished else { return }
        flow.inReadBatch = true
        if flow.discardInput {
            return
        }
        do {
            guard !flow.inputEnded else { throw HTTP3ProxyError.protocolViolation }
            switch message {
            case let .request(.head(head)):
                try requestHead(head)
            case let .response(.head(head)):
                try responseHead(head)
            default:
                try receiveContent(message, flow: flow)
            }
        } catch { fail(error) }
    }

    private func receiveContent(_ message: HTTP3Message, flow: HTTP3Flow) throws {
        guard flow.head != nil else { throw HTTP3ProxyError.protocolViolation }
        if let body = message.body {
            try captureBody(body, flow: flow, representation: .original)
        }
        if message.endsMessage {
            try originalEnd(message, flow: flow)
            flow.inputEnded = true
        }
        if flow.waitingForControl {
            try holdInput(message, flow: flow)
        } else {
            try consume(message, flow: flow)
        }
    }

    func requestHead(_ head: HTTPRequest) throws {
        try HTTP3Fields.validate(head.headerFields)
        guard request.head == nil, head.method.rawValue != "CONNECT", head.extendedConnectProtocol == nil,
              head.scheme == "https", let authority = HTTP3Fields.requestAuthority(head),
              authority.authority == transport.configuration.route.authority,
              let path = head.path, path.hasPrefix("/") || path == "*" else {
            throw HTTP3ProxyError.unsupportedProtocol
        }
        context = HTTPControlContext(
            requestID: id,
            authority: authority.authority,
            method: head.method.rawValue,
            path: path,
            isHTTP2OrLater: true
        )
        if let runtime = transport.controlRuntime {
            let snapshot = runtime.configurationSnapshot
            configurationWork = try HTTPControlWorkLifetime(
                configuration: snapshot,
                runtime: runtime,
                admission: transport.localAdmission,
                transportAdmission: transport.sharedAdmission
            )
            configuration = snapshot
        }
        try beginHead(.request(.head(head)), fields: head.headerFields, flow: request)
    }

    func responseHead(_ head: HTTPResponse) throws {
        try HTTP3Fields.validate(head.headerFields)
        guard response.head == nil, head.status.code != 101 else { throw HTTP3ProxyError.protocolViolation }
        if head.status.code < 200 {
            let snapshot = try HTTP3HeaderSnapshot(head.headerFields, resources: transport.resources)
            withExtendedLifetime(snapshot) {
                captureHead(.response(.head(head)), fields: snapshot.fields, representation: .original)
            }
            try enqueue(.response(.head(head)), flow: response)
            return
        }
        try beginHead(.response(.head(head)), fields: head.headerFields, flow: response)
    }

    func beginHead(_ message: HTTP3Message, fields: HTTPFields, flow: HTTP3Flow) throws {
        flow.headLease = try transport.resources.reserve(message.retainedBytes, kind: .forwarding)
        let snapshot = try HTTP3HeaderSnapshot(fields, resources: transport.resources)
        flow.snapshot = snapshot
        flow.head = message
        captureHead(message, fields: snapshot.fields, representation: .original)
        try evaluateHead(flow)
    }

    func originalEnd(_ message: HTTP3Message, flow: HTTP3Flow) throws {
        let fields = trailers(message)
        if let fields {
            try HTTP3Fields.validate(fields)
            guard !originalFailed else { return }
            let snapshot = try HTTP3HeaderSnapshot(fields, resources: transport.resources)
            withExtendedLifetime(snapshot) {
                captureTrailers(snapshot.fields, flow: flow, representation: .original)
            }
        }
        captureEnd(flow, representation: .original)
    }

    func consume(_ message: HTTP3Message, flow: HTTP3Flow) throws {
        if let maximum = flow.body.map({ _ in flow.bodyMaximum }) {
            if let bytes = message.body {
                guard bytes.readableBytes <= maximum - (flow.body?.count ?? 0) else {
                    throw HTTPControlError.bodyLimitExceeded
                }
                flow.body?.append(contentsOf: bytes.readableBytesView)
            } else if message.endsMessage {
                try completeCollectedBody(message, flow: flow)
            }
            return
        }
        if message.endsMessage {
            let held = try effectiveEnd(message, flow: flow)
            try enqueue(held.message, lease: held.lease, flow: flow)
        } else {
            try enqueue(message, flow: flow)
        }
    }

    func trailers(_ message: HTTP3Message) -> HTTPFields? {
        switch message {
        case let .request(.end(fields)), let .response(.end(fields)): fields
        default: nil
        }
    }

    func readComplete(_ flow: HTTP3Flow) {
        flow.reading = false
        flow.inReadBatch = false
        readIfReady(flow)
    }

    func readIfReady(_ flow: HTTP3Flow) {
        guard !failed, !finished, !flow.inputClosed, !flow.discardInput, !flow.reading, !flow.inReadBatch,
              !flow.waitingForControl, !flow.writing, flow.input.isEmpty, flow.output.isEmpty,
              flow.generatedBody == nil, let source = flow.source, source.isActive,
              flow.head == nil || flow.body != nil || flow.outputEnded || flow.destination?.isWritable == true else {
            return
        }
        flow.reading = true
        source.read()
    }

    func inputClosed(_ flow: HTTP3Flow) {
        flow.inputClosed = true
        if !flow.inputEnded {
            fail(HTTP3ProxyError.protocolViolation)
        } else {
            finishIfReady()
        }
    }

    func inactive(_ flow: HTTP3Flow) {
        flow.sourceInactive = true
        guard !failed, !finished else { return }
        if !flow.inputEnded || !flow.inputDisposed {
            fail(HTTP3ProxyError.protocolViolation)
        } else {
            let outgoing = flow.isRequest ? response : request
            if !outgoing.outputEnded, !outgoing.writing {
                fail(HTTP3ProxyError.protocolViolation)
            } else {
                finishIfReady()
            }
        }
    }
}

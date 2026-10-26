import Foundation
import Synchronization
import SwiftMITM
import SwiftMITMInspection

struct ConsoleCaptureSink: CaptureEventSink {
    let output: ConsoleOutput

    func receive(_ event: CaptureEvent) {
        switch event {
        case let .requestHead(head):
            output.emit("request \(head.method.prefix(256)) \(head.authority.prefix(256))")
        case let .responseHead(head):
            output.emit("response \(head.status) requestID=\(head.requestID)")
        case let .streamError(id, _): output.emit("streamError requestID=\(id)")
        case let .connectionFailure(failure):
            output.emit(
                "connectionFailure reason=\(failure.reason.rawValue) "
                    + "stage=\(failure.stage?.rawValue ?? "unknown")"
            )
        case .webSocketOpen, .webSocketFrame, .webSocketClose: output.emit("webSocket event")
        default: break
        }
    }
}

struct ConsoleInspectionSink: InspectionEventSink {
    let output: ConsoleOutput

    func receive(_ event: InspectionEvent) {
        let summary: String
        switch event.payload {
        case .head: summary = "head"
        case .trailers: summary = "trailers"
        case let .decodedBody(bytes): summary = "decodedBody bytes=\(bytes.count)"
        case let .sse(value): summary = "sse dataBytes=\(value.data.utf8.count)"
        case .grpc: summary = "grpc event"
        case let .protobuf(name, digest, fields):
            summary = "protobuf message=\(name) schema=\(digest) fields=\(fields.count)"
        case let .webSocketChunk(bytes): summary = "webSocketChunk bytes=\(bytes.count)"
        case let .webSocketMessage(message): summary = "webSocketMessage bytes=\(message.byteCount)"
        case .diagnostic: summary = "diagnostic"
        case .end: summary = "end"
        }
        output.emit(
            "inspection \(event.requestID) \(event.direction) \(event.representation) "
                + "seq=\(event.sequence) \(summary)"
        )
    }
}

struct ConsoleBreakpointSink: HTTPControlEventSink {
    let output: ConsoleOutput
    private let continuation: AsyncStream<BreakpointHandle>.Continuation
    let handles: AsyncStream<BreakpointHandle>

    init(output: ConsoleOutput) {
        self.output = output
        let pair = AsyncStream<BreakpointHandle>.makeStream(bufferingPolicy: .bufferingOldest(16))
        handles = pair.stream
        continuation = pair.continuation
    }

    func receive(_ event: HTTPControlEvent) {
        if case let .breakpoint(handle) = event {
            continuation.yield(handle)
            output.emit("breakpoint queued requestID=\(handle.requestID)")
        }
    }

    func finish() { continuation.finish() }
}

actor ReplayConsoleSink: ReplayResponseSink {
    let output: ConsoleOutput
    private(set) var responseHead: CapturedResponseHead?

    init(output: ConsoleOutput) { self.output = output }

    func receive(_ event: ReplayResponseEvent) async throws {
        switch event {
        case let .started(_, requestID, sourceRequestID, representation):
            output.emit(
                "replay started requestID=\(requestID) sourceID=\(sourceRequestID) "
                    + "representation=\(representation)"
            )
        case let .head(_, head):
            responseHead = head
            output.emit("replay response head status=\(head.status)")
        case let .body(_, id, bytes):
            output.emit("replay response body requestID=\(id) bytes=\(bytes.count)")
        case let .trailers(_, id, headers):
            output.emit("replay response trailers requestID=\(id) count=\(headers.count)")
        case let .end(result):
            output.emit("replay complete status=\(result.status) bytes=\(result.responseBodyBytes)")
        }
    }
}

actor FileHARWriter: HAROutputWriter {
    private let handle: FileHandle

    init(path: String) throws {
        guard FileManager.default.createFile(atPath: path, contents: nil) else { throw LabError.outputFile }
        handle = try FileHandle(forWritingTo: URL(fileURLWithPath: path))
        try handle.truncate(atOffset: 0)
    }

    func write(_ bytes: [UInt8]) async throws {
        try handle.write(contentsOf: Data(bytes))
    }

    func close() throws { try handle.close() }
}

final class ReplayCaptureSink: CaptureEventSink {
    private let captured = Mutex<CapturedRequestHead?>(nil)
    private let console: ConsoleCaptureSink

    init(output: ConsoleOutput) { console = ConsoleCaptureSink(output: output) }

    var requestHead: CapturedRequestHead? { captured.withLock { $0 } }

    func receive(_ event: CaptureEvent) {
        if case let .requestHead(head) = event {
            captured.withLock { $0 = head }
        }
        console.receive(event)
    }
}

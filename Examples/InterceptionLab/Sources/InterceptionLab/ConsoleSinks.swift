import Foundation
import Synchronization
import SwiftMITM
import SwiftMITMInspection

struct ConsoleCaptureSink: CaptureEventSink {
    let output: ConsoleOutput

    func receive(_ event: CaptureEvent) {
        switch event {
        case .requestHead(let head):
            output.emit("request \(head.method.prefix(256)) \(head.authority.prefix(256))")
        case .responseHead(let head):
            output.emit("response \(head.status) requestID=\(head.requestID)")
        case .streamError(let id, _): output.emit("streamError requestID=\(id)")
        case .connectionFailure(let failure):
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
        case .decodedBody(let bytes): summary = "decodedBody bytes=\(bytes.count)"
        case .sse(let value): summary = "sse dataBytes=\(value.data.utf8.count)"
        case .grpc: summary = "grpc event"
        case .protobuf(let name, let digest, let fields):
            summary = "protobuf message=\(name) schema=\(digest) fields=\(fields.count)"
        case .webSocketChunk(let bytes): summary = "webSocketChunk bytes=\(bytes.count)"
        case .webSocketMessage(let message): summary = "webSocketMessage bytes=\(message.byteCount)"
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
        if case .breakpoint(let handle) = event {
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
        case .started(_, let requestID, let sourceRequestID, let representation):
            output.emit(
                "replay started requestID=\(requestID) sourceID=\(sourceRequestID) "
                    + "representation=\(representation)"
            )
        case .head(_, let head):
            responseHead = head
            output.emit("replay response head status=\(head.status)")
        case .body(_, let id, let bytes):
            output.emit("replay response body requestID=\(id) bytes=\(bytes.count)")
        case .trailers(_, let id, let headers):
            output.emit("replay response trailers requestID=\(id) count=\(headers.count)")
        case .end(let result):
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
        if case .requestHead(let head) = event {
            captured.withLock { $0 = head }
        }
        console.receive(event)
    }
}

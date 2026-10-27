import Dispatch
import Foundation
import Logging
import NIOConcurrencyHelpers

final class HostWriteTrace: Sendable {
    private struct Record: Encodable {
        let event: String
        let uptimeNanoseconds: UInt64
        let metadata: [String: String]
    }

    private struct Summary: Encodable {
        let schema = "SwiftMITMHTTP3.writeTrace.v1"
        let records: Int
        let truncated: Bool
        let encodingFailed: Bool
    }

    private struct State: Sendable {
        var data = Data()
        var records = 0
        var truncated = false
        var encodingFailed = false
    }

    private enum Failure: Error {
        case encoding
    }

    private static let events: Set<String> = [
        "swiftmitm.http3.write.submit",
        "swiftmitm.http3.write.complete.success",
        "swiftmitm.http3.write.complete.failure",
        "swiftmitm.http3.writability.changed"
    ]
    private static let keys = [
        "exchange",
        "direction",
        "outputQueueCount",
        "pendingPayloadBytes",
        "writing",
        "writable",
        "messageKind"
    ]
    private let state = NIOLockedValueBox(State())

    func bootstrap() {
        LoggingSystem.bootstrap { label in HostWriteTraceHandler(label: label, trace: self) }
    }

    func receive(_ event: LogEvent) {
        let name = event.message.description
        guard Self.events.contains(name) else { return }
        let fields = Dictionary(uniqueKeysWithValues: Self.keys.compactMap { key in
            event.metadata?[key].map { (key, $0.description) }
        })
        state.withLockedValue {
            guard !$0.truncated, !$0.encodingFailed else { return }
            let record = Record(event: name, uptimeNanoseconds: DispatchTime.now().uptimeNanoseconds, metadata: fields)
            do {
                let encoded = try JSONEncoder().encode(record)
                guard $0.records < 4096, $0.data.count + encoded.count + 1 <= 1024 * 1024 - 1024 else {
                    $0.truncated = true
                    return
                }
                $0.data.append(encoded)
                $0.data.append(0x0A)
                $0.records += 1
            } catch {
                $0.encodingFailed = true
            }
        }
    }

    func write(to path: String) throws {
        let snapshot = state.withLockedValue { $0 }
        var data = snapshot.data
        let summary = Summary(
            records: snapshot.records,
            truncated: snapshot.truncated,
            encodingFailed: snapshot.encodingFailed
        )
        data.append(try JSONEncoder().encode(summary))
        data.append(0x0A)
        try data.write(to: URL(fileURLWithPath: path), options: .atomic)
        if snapshot.encodingFailed {
            throw Failure.encoding
        }
    }
}

private struct HostWriteTraceHandler: LogHandler {
    let label: String
    let trace: HostWriteTrace
    var metadata: Logger.Metadata = [:]
    var metadataProvider: Logger.MetadataProvider?
    var logLevel: Logger.Level

    init(label: String, trace: HostWriteTrace) {
        self.label = label
        self.trace = trace
        logLevel = label == "SwiftMITM.HTTP3" ? .trace : .critical
    }

    subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { metadata[key] }
        set { metadata[key] = newValue }
    }

    func log(event: LogEvent) {
        guard label == "SwiftMITM.HTTP3", event.level == .trace else { return }
        trace.receive(event)
    }
}

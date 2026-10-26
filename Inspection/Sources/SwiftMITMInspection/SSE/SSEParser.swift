public struct SSEEvent: Sendable {
    public let data: String
    public let event: String
    public let lastEventID: String
    public let retryMilliseconds: String?
}

public enum SSEParserError: Error, Sendable {
    case invalidConfiguration
    case lineLimitExceeded
    case eventLimitExceeded
    case alreadyFinished
}

public enum SSEEndState: Sendable {
    case complete
    case unfinishedRecord
}

public final class SSEParser {
    public let reservedStorageBytes: Int
    private let budget: InspectionMemoryBudget
    public private(set) var lastEventID = ""
    public private(set) var retryMilliseconds: String?
    private let maximumLineBytes: Int
    private let maximumEventBytes: Int
    private var line: [UInt8] = []
    private var data: [UInt8] = []
    private var event = ""
    private var firstLine = true
    private var skipLF = false
    private var finished = false
    private var failed = false

    public init(
        maximumLineBytes: Int, maximumEventBytes: Int, budget: InspectionMemoryBudget
    ) throws {
        guard maximumLineBytes > 0, maximumLineBytes <= 262_144,
            maximumEventBytes > 0, maximumEventBytes <= 4_194_304
        else { throw SSEParserError.invalidConfiguration }
        // Charge carry capacity, UTF-8 replacement expansion, metadata and synchronous output copies.
        let reservation = 8 * maximumLineBytes + 4 * maximumEventBytes
        guard budget.reserve(reservation) else { throw InspectionBudgetError.capacityExceeded }
        self.budget = budget
        reservedStorageBytes = reservation
        self.maximumLineBytes = maximumLineBytes
        self.maximumEventBytes = maximumEventBytes
    }

    deinit { budget.release(reservedStorageBytes) }

    public func feed(
        _ input: UnsafeRawBufferPointer,
        emit: (SSEEvent) throws -> Void
    ) throws {
        guard !finished, !failed else { throw SSEParserError.alreadyFinished }
        do {
            for byte in input {
                if skipLF {
                    skipLF = false
                    if byte == 10 {
                        continue
                    }
                }
                if byte == 10 || byte == 13 {
                    try processLine(emit: emit)
                    line.removeAll(keepingCapacity: true)
                    skipLF = byte == 13
                } else {
                    guard line.count < maximumLineBytes else { throw SSEParserError.lineLimitExceeded }
                    line.append(byte)
                }
            }
        } catch {
            failed = true
            throw error
        }
    }

    public func finish() -> SSEEndState {
        let partial = !line.isEmpty || !data.isEmpty || !event.isEmpty
        finished = true
        line.removeAll(keepingCapacity: false)
        data.removeAll(keepingCapacity: false)
        event = ""
        return partial ? .unfinishedRecord : .complete
    }

    private func processLine(emit: (SSEEvent) throws -> Void) throws {
        var bytes = line[...]
        if firstLine {
            firstLine = false
            if bytes.starts(with: [0xEF, 0xBB, 0xBF]) {
                bytes = bytes.dropFirst(3)
            }
        }
        if bytes.isEmpty {
            try dispatch(emit: emit)
            return
        }
        let decoded = String(decoding: bytes, as: UTF8.self)
        let decodedCount = decoded.utf8.count
        guard decodedCount <= maximumLineBytes else { throw SSEParserError.lineLimitExceeded }
        if decoded.first == ":" {
            return
        }
        let (field, value) = Self.field(decoded)
        try apply(field: field, value: value)
    }

    private func dispatch(emit: (SSEEvent) throws -> Void) throws {
        if !data.isEmpty {
            data.removeLast()
            try emit(SSEEvent(
                data: String(decoding: data, as: UTF8.self),
                event: event.isEmpty ? "message" : event,
                lastEventID: lastEventID,
                retryMilliseconds: retryMilliseconds
            ))
        }
        data.removeAll(keepingCapacity: true)
        event = ""
    }

    private static func field(_ decoded: String) -> (name: Substring, value: Substring) {
        if let colon = decoded.firstIndex(of: ":") {
            let field = decoded[..<colon]
            var value = decoded[decoded.index(after: colon)...]
            if value.first == " " {
                value = value.dropFirst()
            }
            return (field, value)
        }
        return (decoded[...], "")
    }

    private func apply(field: Substring, value: Substring) throws {
        switch field {
        case "data":
            guard value.utf8.count <= maximumEventBytes - data.count - event.utf8.count else {
                throw SSEParserError.eventLimitExceeded
            }
            data.append(contentsOf: value.utf8)
            data.append(10)
        case "event":
            guard value.utf8.count <= maximumEventBytes - max(0, data.count - 1) else {
                throw SSEParserError.eventLimitExceeded
            }
            event = String(value)
        case "id":
            if !value.utf8.contains(0) {
                lastEventID = String(value)
            }
        case "retry":
            if !value.isEmpty && value.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }) {
                retryMilliseconds = String(value)
            }
        default:
            break
        }
    }
}

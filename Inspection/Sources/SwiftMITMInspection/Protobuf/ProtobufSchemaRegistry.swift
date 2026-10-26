import Crypto
import Foundation
import SwiftProtobuf

public struct ProtobufSchemaLimits: Sendable {
    public let descriptorBytes: Int
    public let files: Int
    public let fields: Int
    public let depth: Int

    public init(
        descriptorBytes: Int = 4 * 1024 * 1024, files: Int = 128, fields: Int = 10_000, depth: Int = 32
    ) throws {
        guard (1...(16 * 1024 * 1024)).contains(descriptorBytes), (1...512).contains(files),
              (1...50_000).contains(fields), (1...64).contains(depth) else {
            throw ProtobufInspectionError.limitExceeded
        }
        self.descriptorBytes = descriptorBytes
        self.files = files
        self.fields = fields
        self.depth = depth
    }
}

public final class ProtobufSchemaSnapshot: Sendable {
    public let digest: String
    public var messageNames: [String] { messages.keys.sorted() }
    private let messages: [String: ProtobufMessageSchema]
    private let enums: [String: [Int32: String]]
    private let methods: [String: (input: String, output: String)]
    private let budget: InspectionMemoryBudget
    private let reservation: Int

    init(
        digest: String,
        messages: [String: ProtobufMessageSchema],
        enums: [String: [Int32: String]],
        methods: [String: (input: String, output: String)],
        budget: InspectionMemoryBudget,
        reservation: Int
    ) {
        self.digest = digest
        self.messages = messages
        self.enums = enums
        self.methods = methods
        self.budget = budget
        self.reservation = reservation
    }

    deinit { budget.release(reservation) }

    public func messageName(methodPath: String, response: Bool, explicitName: String? = nil) -> String? {
        if let explicitName {
            let name = explicitName.hasPrefix(".") ? explicitName : "." + explicitName
            return messages[name] == nil ? nil : name
        }
        guard let method = methods[methodPath] else { return nil }
        return response ? method.output : method.input
    }

    public func decode(
        _ bytes: [UInt8], messageName: String, maximumNodes: Int = 20_000, maximumDepth: Int = 32
    ) throws -> [ProtobufFieldOccurrence] {
        guard (1...80_000).contains(maximumNodes), (1...64).contains(maximumDepth),
              bytes.count <= 16 * 1024 * 1024 else { throw ProtobufInspectionError.limitExceeded }
        let charge = bytes.count * 2 + maximumNodes * 256
        guard budget.reserve(charge) else { throw ProtobufInspectionError.capacityExceeded }
        defer { budget.release(charge) }
        var interpreter = ProtobufWireInterpreter(
            bytes: bytes,
            messages: messages,
            enums: enums,
            maximumNodes: maximumNodes,
            maximumDepth: maximumDepth
        )
        let name = messageName.hasPrefix(".") ? messageName : "." + messageName
        return try interpreter.decode(0..<bytes.count, messageName: name)
    }
}

public actor ProtobufSchemaRegistry {
    private final class Reference {
        weak var snapshot: ProtobufSchemaSnapshot?
        init(_ snapshot: ProtobufSchemaSnapshot) { self.snapshot = snapshot }
    }

    private let budget: InspectionMemoryBudget
    private let limits: ProtobufSchemaLimits
    private var references: [Reference] = []
    public private(set) var current: ProtobufSchemaSnapshot?

    public init(budget: InspectionMemoryBudget, limits: ProtobufSchemaLimits) {
        self.budget = budget
        self.limits = limits
    }

    public func load(descriptorBytes: [UInt8]) throws -> ProtobufSchemaSnapshot {
        references.removeAll { $0.snapshot == nil }
        guard references.count < 4 else { throw ProtobufInspectionError.capacityExceeded }
        guard descriptorBytes.count <= limits.descriptorBytes else { throw ProtobufInspectionError.limitExceeded }
        var admission = DescriptorAdmission(bytes: descriptorBytes, budget: budget, maximumDepth: limits.depth)
        do {
            try admission.scan()
            var options = BinaryDecodingOptions()
            options.messageDepthLimit = limits.depth + 8
            let set = try Google_Protobuf_FileDescriptorSet(serializedBytes: descriptorBytes, options: options)
            var builder = ProtobufSchemaBuilder(limits: limits, budget: budget)
            defer { budget.release(builder.reserved) }
            try builder.build(set)
            let digest = SHA256.hash(data: Data(descriptorBytes)).map { String(format: "%02x", $0) }.joined()
            let snapshot = ProtobufSchemaSnapshot(
                digest: digest,
                messages: builder.messages,
                enums: builder.enums,
                methods: builder.methods,
                budget: budget,
                reservation: admission.reserved + builder.reserved
            )
            admission.reserved = 0
            builder.reserved = 0
            references.append(Reference(snapshot))
            current = snapshot
            return snapshot
        } catch {
            budget.release(admission.reserved)
            throw error
        }
    }

    public func select(_ snapshot: ProtobufSchemaSnapshot) throws {
        guard references.contains(where: { $0.snapshot === snapshot }) else {
            throw ProtobufInspectionError.schemaUnavailable
        }
        current = snapshot
    }
}

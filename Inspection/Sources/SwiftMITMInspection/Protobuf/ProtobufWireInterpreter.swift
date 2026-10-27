import Foundation

public enum ProtobufInspectionError: Error, Sendable {
    case limitExceeded, malformedWire, schemaUnavailable, schemaMismatch
    case invalidDescriptor, unsupportedSchema, missingImport, duplicateSymbol, capacityExceeded
}

indirect public enum ProtobufValue: Sendable {
    case unsigned(UInt64), signed(Int64), floating(Double), boolean(Bool), string(String), bytes([UInt8])
    case enumeration(number: Int32, name: String?)
    case message([ProtobufFieldOccurrence]), packed([ProtobufValue])
    case unknown(wireType: Int, bytes: [UInt8])
}

public struct ProtobufFieldOccurrence: Sendable {
    public let number: Int
    public let name: String?
    public let oneof: String?
    public let repeated: Bool
    public let mapEntry: Bool
    public let range: Range<Int>
    public let value: ProtobufValue
}

struct ProtobufField: Sendable {
    let name: String
    let number: Int
    let type: Int
    let typeName: String?
    let repeated: Bool
    let oneof: String?
}

struct ProtobufMessageSchema: Sendable {
    let fields: [Int: ProtobufField]
    let mapEntry: Bool
}

struct ProtobufWireInterpreter {
    let bytes: [UInt8]
    let messages: [String: ProtobufMessageSchema]
    let enums: [String: [Int32: String]]
    let maximumNodes: Int
    let maximumDepth: Int
    var nodes = 0

    mutating func decode(_ range: Range<Int>, messageName: String, depth: Int = 0) throws -> [ProtobufFieldOccurrence] {
        guard depth <= maximumDepth else { throw ProtobufInspectionError.limitExceeded }
        guard let schema = messages[messageName] else { throw ProtobufInspectionError.schemaUnavailable }
        var result: [ProtobufFieldOccurrence] = []
        var cursor = range.lowerBound
        while cursor < range.upperBound {
            try chargeNode()
            let start = cursor
            let tag = try varint(&cursor, end: range.upperBound)
            let number = Int(tag >> 3)
            let wire = Int(tag & 7)
            guard number > 0, number <= 536_870_911 else { throw ProtobufInspectionError.malformedWire }
            let field = schema.fields[number]
            let value: ProtobufValue
            if let field {
                if wire == 2, field.repeated, field.type != 9, field.type != 11, field.type != 12, field.type != 10 {
                    let packedRange = try lengthRange(&cursor, end: range.upperBound)
                    var packedCursor = packedRange.lowerBound
                    var packed: [ProtobufValue] = []
                    while packedCursor < packedRange.upperBound {
                        try chargeNode()
                        packed.append(try scalar(
                            field,
                            wire: scalarWire(field.type),
                            cursor: &packedCursor,
                            end: packedRange.upperBound,
                            depth: depth
                        ))
                    }
                    value = .packed(packed)
                } else {
                    value = try scalar(field, wire: wire, cursor: &cursor, end: range.upperBound, depth: depth)
                }
            } else {
                try skip(wire: wire, number: number, cursor: &cursor, end: range.upperBound, depth: depth)
                value = .unknown(wireType: wire, bytes: Array(bytes[start..<cursor]))
            }
            result.append(ProtobufFieldOccurrence(
                number: number,
                name: field?.name,
                oneof: field?.oneof,
                repeated: field?.repeated ?? false,
                mapEntry: field?.typeName.flatMap { messages[$0]?.mapEntry } ?? false,
                range: start..<cursor,
                value: value
            ))
        }
        return result
    }

    private mutating func scalar(
        _ field: ProtobufField,
        wire: Int,
        cursor: inout Int,
        end: Int,
        depth: Int
    ) throws -> ProtobufValue {
        guard wire == scalarWire(field.type) else { throw ProtobufInspectionError.schemaMismatch }
        switch field.type {
        case 1, 2, 6, 7, 15, 16: return try fixedScalar(field.type, cursor: &cursor, end: end)
        case 3, 4, 5, 8, 13, 14, 17, 18: return try varintScalar(field, cursor: &cursor, end: end)
        case 9:
            let range = try lengthRange(&cursor, end: end)
            guard let text = String(bytes: bytes[range], encoding: .utf8) else {
                throw ProtobufInspectionError.schemaMismatch
            }
            return .string(text)
        case 11:
            let range = try lengthRange(&cursor, end: end)
            guard let name = field.typeName else { throw ProtobufInspectionError.schemaMismatch }
            return .message(try decode(range, messageName: name, depth: depth + 1))
        case 12: return .bytes(Array(bytes[try lengthRange(&cursor, end: end)]))
        default: throw ProtobufInspectionError.unsupportedSchema
        }
    }

    private func fixedScalar(_ type: Int, cursor: inout Int, end: Int) throws -> ProtobufValue {
        switch type {
        case 1: return .floating(Double(bitPattern: try fixed(&cursor, count: 8, end: end)))
        case 2: return .floating(Double(Float(bitPattern: UInt32(try fixed(&cursor, count: 4, end: end)))))
        case 6: return .unsigned(try fixed(&cursor, count: 8, end: end))
        case 7: return .unsigned(try fixed(&cursor, count: 4, end: end))
        case 15: return .signed(Int64(Int32(bitPattern: UInt32(try fixed(&cursor, count: 4, end: end)))))
        case 16: return .signed(Int64(bitPattern: try fixed(&cursor, count: 8, end: end)))
        default: throw ProtobufInspectionError.unsupportedSchema
        }
    }

    private func varintScalar(_ field: ProtobufField, cursor: inout Int, end: Int) throws -> ProtobufValue {
        switch field.type {
        case 3: return .signed(Int64(bitPattern: try varint(&cursor, end: end)))
        case 4: return .unsigned(try varint(&cursor, end: end))
        case 5: return .signed(Int64(Int32(truncatingIfNeeded: try varint(&cursor, end: end))))
        case 8: return .boolean(try varint(&cursor, end: end) != 0)
        case 13: return .unsigned(UInt64(UInt32(truncatingIfNeeded: try varint(&cursor, end: end))))
        case 14:
            let number = Int32(truncatingIfNeeded: try varint(&cursor, end: end))
            return .enumeration(number: number, name: field.typeName.flatMap { enums[$0]?[number] })
        case 17:
            let value = UInt32(truncatingIfNeeded: try varint(&cursor, end: end))
            return .signed(Int64(Int32(bitPattern: (value >> 1) ^ (0 &- (value & 1)))))
        case 18:
            let value = try varint(&cursor, end: end)
            return .signed(Int64(bitPattern: (value >> 1) ^ (0 &- (value & 1))))
        default: throw ProtobufInspectionError.unsupportedSchema
        }
    }

    private func scalarWire(_ type: Int) -> Int {
        switch type {
        case 1, 6, 16: 1
        case 2, 7, 15: 5
        case 9, 11, 12: 2
        case 10: 3
        default: 0
        }
    }

    private mutating func skip(wire: Int, number: Int, cursor: inout Int, end: Int, depth: Int) throws {
        switch wire {
        case 0: _ = try varint(&cursor, end: end)
        case 1: _ = try fixed(&cursor, count: 8, end: end)
        case 2: _ = try lengthRange(&cursor, end: end)
        case 3:
            try skipGroup(number: number, cursor: &cursor, end: end, depth: depth)
        case 5: _ = try fixed(&cursor, count: 4, end: end)
        default: throw ProtobufInspectionError.malformedWire
        }
    }

    private mutating func skipGroup(number: Int, cursor: inout Int, end: Int, depth: Int) throws {
        guard depth < maximumDepth else { throw ProtobufInspectionError.limitExceeded }
        while cursor < end {
            try chargeNode()
            let tag = try varint(&cursor, end: end)
            let nestedNumber = Int(tag >> 3)
            let nestedWire = Int(tag & 7)
            guard nestedNumber > 0, nestedNumber <= 536_870_911 else {
                throw ProtobufInspectionError.malformedWire
            }
            if nestedWire == 4 {
                guard nestedNumber == number else { throw ProtobufInspectionError.malformedWire }
                return
            }
            try skip(wire: nestedWire, number: nestedNumber, cursor: &cursor, end: end, depth: depth + 1)
        }
        throw ProtobufInspectionError.malformedWire
    }

    private mutating func chargeNode() throws {
        guard nodes < maximumNodes else { throw ProtobufInspectionError.limitExceeded }
        nodes += 1
    }

    private func varint(_ cursor: inout Int, end: Int) throws -> UInt64 {
        var result: UInt64 = 0
        for index in 0..<10 {
            guard cursor < end else { throw ProtobufInspectionError.malformedWire }
            let byte = bytes[cursor]
            cursor += 1
            guard index != 9 || byte <= 1 else { throw ProtobufInspectionError.malformedWire }
            result |= UInt64(byte & 127) << (index * 7)
            if byte & 128 == 0 {
                return result
            }
        }
        throw ProtobufInspectionError.malformedWire
    }

    private func fixed(_ cursor: inout Int, count: Int, end: Int) throws -> UInt64 {
        guard count <= end - cursor else { throw ProtobufInspectionError.malformedWire }
        var result: UInt64 = 0
        for index in 0..<count { result |= UInt64(bytes[cursor + index]) << (index * 8) }
        cursor += count
        return result
    }

    private func lengthRange(_ cursor: inout Int, end: Int) throws -> Range<Int> {
        let length = try varint(&cursor, end: end)
        guard length <= UInt64(end - cursor) else { throw ProtobufInspectionError.malformedWire }
        let range = cursor..<(cursor + Int(length))
        cursor = range.upperBound
        return range
    }
}

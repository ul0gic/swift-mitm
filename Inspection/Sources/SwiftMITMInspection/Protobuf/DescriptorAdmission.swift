struct DescriptorAdmission {
    let bytes: [UInt8]
    let budget: InspectionMemoryBudget
    let maximumDepth: Int
    var reserved = 0

    mutating func scan() throws {
        let charge = bytes.count * 4
        guard budget.reserve(charge) else { throw ProtobufInspectionError.capacityExceeded }
        reserved += charge
        try scan(0..<bytes.count, kind: 0, depth: 0)
    }

    private mutating func scan(_ range: Range<Int>, kind: Int, depth: Int) throws {
        guard depth <= maximumDepth + 8 else { throw ProtobufInspectionError.limitExceeded }
        var cursor = range.lowerBound
        while cursor < range.upperBound {
            guard budget.reserve(256) else { throw ProtobufInspectionError.capacityExceeded }
            reserved += 256
            let tag = try varint(&cursor, end: range.upperBound)
            let number = Int(tag >> 3)
            guard number > 0, number <= 536_870_911 else { throw ProtobufInspectionError.invalidDescriptor }
            switch tag & 7 {
            case 0: _ = try varint(&cursor, end: range.upperBound)
            case 1:
                guard range.upperBound - cursor >= 8 else { throw ProtobufInspectionError.invalidDescriptor }
                cursor += 8
            case 5:
                guard range.upperBound - cursor >= 4 else { throw ProtobufInspectionError.invalidDescriptor }
                cursor += 4
            case 2:
                let length = try varint(&cursor, end: range.upperBound)
                guard length <= UInt64(range.upperBound - cursor) else {
                    throw ProtobufInspectionError.invalidDescriptor
                }
                let nested = cursor..<(cursor + Int(length))
                cursor = nested.upperBound
                if let next = child(kind: kind, field: number) { try scan(nested, kind: next, depth: depth + 1) }
            default: throw ProtobufInspectionError.unsupportedSchema
            }
        }
    }

    private func child(kind: Int, field: Int) -> Int? {
        switch kind {
        case 0: return field == 1 ? 1 : nil
        case 1: return fileChild(field)
        case 2: return messageChild(field)
        case 3...5: return declarationChild(kind: kind, field: field)
        case 6...9: return serviceChild(kind: kind, field: field)
        default: return optionChild(kind: kind, field: field)
        }
    }

    private func fileChild(_ field: Int) -> Int? {
        switch field {
        case 4: 2
        case 5: 4
        case 6: 6
        case 7: 3
        case 8: 20
        case 9: 9
        default: nil
        }
    }

    private func messageChild(_ field: Int) -> Int? {
        switch field {
        case 3: 2
        case 4: 4
        case 6, 2: 3
        case 8: 8
        case 5, 9: 11
        case 7: 21
        default: nil
        }
    }

    private func declarationChild(kind: Int, field: Int) -> Int? {
        switch (kind, field) {
        case (3, 8): 22
        case (4, 4): 11
        case (4, 3): 23
        case (4, 2): 5
        case (5, 3): 24
        default: nil
        }
    }

    private func serviceChild(kind: Int, field: Int) -> Int? {
        switch (kind, field) {
        case (6, 2): 7
        case (6, 3): 25
        case (7, 4): 26
        case (8, 2): 27
        case (9, 1): 10
        default: nil
        }
    }

    private func optionChild(kind: Int, field: Int) -> Int? {
        switch (kind, field) {
        case (20...27, 999): 28
        case (20, 50), (21, 12), (22, 21), (23, 7), (24, 2), (25, 34), (26, 35), (27, 1): 29
        case (22, 20): 30
        case (22, 22), (24, 4): 31
        case (28, 2): 32
        default: nil
        }
    }

    private func varint(_ cursor: inout Int, end: Int) throws -> UInt64 {
        var value: UInt64 = 0
        for index in 0..<10 {
            guard cursor < end else { throw ProtobufInspectionError.invalidDescriptor }
            let byte = bytes[cursor]
            cursor += 1
            guard index != 9 || byte <= 1 else { throw ProtobufInspectionError.invalidDescriptor }
            value |= UInt64(byte & 127) << (index * 7)
            if byte < 128 { return value }
        }
        throw ProtobufInspectionError.invalidDescriptor
    }
}

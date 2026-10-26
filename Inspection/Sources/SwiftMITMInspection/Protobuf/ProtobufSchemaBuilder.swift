import SwiftProtobuf

struct ProtobufSchemaBuilder {
    let limits: ProtobufSchemaLimits
    let budget: InspectionMemoryBudget
    var reserved = 0
    var messages: [String: ProtobufMessageSchema] = [:]
    var enums: [String: [Int32: String]] = [:]
    var methods: [String: (input: String, output: String)] = [:]
    private var symbols: Set<String> = []
    private var fieldCount = 0
    private var symbolFiles: [String: String] = [:]
    private var activeFile = ""

    init(limits: ProtobufSchemaLimits, budget: InspectionMemoryBudget) {
        self.limits = limits
        self.budget = budget
    }

    mutating func build(_ set: Google_Protobuf_FileDescriptorSet) throws {
        try validateFiles(set)
        let visibleFiles = try fileVisibility(set)
        try addDefinitions(set)
        try resolveFields(visibleFiles)
        try validateShapes()
        try resolveMethods(visibleFiles)
    }

    private func validateFiles(_ set: Google_Protobuf_FileDescriptorSet) throws {
        guard set.file.count <= limits.files else { throw ProtobufInspectionError.limitExceeded }
        guard set.unknownFields.data.isEmpty else { throw ProtobufInspectionError.unsupportedSchema }
        var files: Set<String> = []
        for file in set.file {
            guard !file.name.isEmpty, files.insert(file.name).inserted else {
                throw ProtobufInspectionError.invalidDescriptor
            }
        }
        guard set.file.allSatisfy({ $0.dependency.allSatisfy { files.contains($0) } }) else {
            throw ProtobufInspectionError.missingImport
        }
        var dependencies = Dictionary(uniqueKeysWithValues: set.file.map { ($0.name, Set($0.dependency)) })
        while !dependencies.isEmpty {
            let ready = Set(dependencies.filter { $0.value.isEmpty }.map(\.key))
            guard !ready.isEmpty else { throw ProtobufInspectionError.invalidDescriptor }
            for name in ready { dependencies.removeValue(forKey: name) }
            for name in dependencies.keys { dependencies[name]?.subtract(ready) }
        }
    }

    private func fileVisibility(
        _ set: Google_Protobuf_FileDescriptorSet
    ) throws -> [String: Set<String>] {
        let fileIndex = Dictionary(uniqueKeysWithValues: set.file.map { ($0.name, $0) })
        var visibleFiles: [String: Set<String>] = [:]
        for file in set.file {
            var visible = Set(file.dependency)
            var pending = file.dependency
            while let name = pending.popLast() {
                guard let imported = fileIndex[name] else { throw ProtobufInspectionError.missingImport }
                for index in imported.publicDependency {
                    guard index >= 0, Int(index) < imported.dependency.count else {
                        throw ProtobufInspectionError.invalidDescriptor
                    }
                    let next = imported.dependency[Int(index)]
                    if visible.insert(next).inserted { pending.append(next) }
                }
            }
            visible.insert(file.name)
            visibleFiles[file.name] = visible
        }
        return visibleFiles
    }

    private mutating func addDefinitions(_ set: Google_Protobuf_FileDescriptorSet) throws {
        for file in set.file {
            activeFile = file.name
            guard file.syntax.isEmpty || file.syntax == "proto2" || file.syntax == "proto3",
                  !file.hasEdition, file.extension.isEmpty, file.unknownFields.data.isEmpty,
                  file.optionDependency.isEmpty, file.options.unknownFields.data.isEmpty,
                  file.options.uninterpretedOption.isEmpty, !file.options.hasFeatures else {
                throw ProtobufInspectionError.unsupportedSchema
            }
            for index in file.publicDependency + file.weakDependency {
                guard index >= 0, Int(index) < file.dependency.count else {
                    throw ProtobufInspectionError.invalidDescriptor
                }
            }
            let prefix = file.package.isEmpty ? "" : "." + file.package
            for value in file.enumType { try addEnum(value, prefix: prefix) }
            for message in file.messageType {
                try addMessage(message, prefix: prefix, depth: 0, proto3: file.syntax == "proto3")
            }
            for service in file.service {
                try addService(service, prefix: prefix)
            }
        }
    }

    private mutating func addService(
        _ service: Google_Protobuf_ServiceDescriptorProto, prefix: String
    ) throws {
        let serviceName = try symbol(service.name, prefix: prefix)
        guard service.unknownFields.data.isEmpty, service.options.unknownFields.data.isEmpty,
            service.options.uninterpretedOption.isEmpty, !service.options.hasFeatures else {
            throw ProtobufInspectionError.unsupportedSchema
        }
        for method in service.method {
            _ = try symbol(method.name, prefix: serviceName)
            guard method.unknownFields.data.isEmpty, method.options.unknownFields.data.isEmpty,
                method.options.uninterpretedOption.isEmpty, !method.options.hasFeatures else {
                throw ProtobufInspectionError.unsupportedSchema
            }
            let path = "/" + String(serviceName.dropFirst()) + "/" + method.name
            methods[path] = (method.inputType, method.outputType)
        }
    }

    private mutating func resolveFields(_ visibleFiles: [String: Set<String>]) throws {
        for (name, schema) in messages {
            var resolved: [Int: ProtobufField] = [:]
            for field in schema.fields.values {
                let target = try field.typeName.map { try resolve($0, scope: name) }
                if let target, let owner = symbolFiles[name], let targetOwner = symbolFiles[target],
                   visibleFiles[owner]?.contains(targetOwner) != true { throw ProtobufInspectionError.missingImport }
                resolved[field.number] = ProtobufField(
                    name: field.name,
                    number: field.number,
                    type: field.type,
                    typeName: target,
                    repeated: field.repeated,
                    oneof: field.oneof
                )
            }
            messages[name] = ProtobufMessageSchema(fields: resolved, mapEntry: schema.mapEntry)
        }
    }

    private func validateShapes() throws {
        for schema in messages.values {
            for field in schema.fields.values {
                if field.type == 11 {
                    guard let name = field.typeName, let target = messages[name] else {
                        throw ProtobufInspectionError.invalidDescriptor
                    }
                    if target.mapEntry && !field.repeated { throw ProtobufInspectionError.invalidDescriptor }
                } else if field.type == 14 {
                    guard let name = field.typeName, enums[name] != nil else {
                        throw ProtobufInspectionError.invalidDescriptor
                    }
                }
            }
            if schema.mapEntry {
                guard schema.fields.count == 2, let key = schema.fields[1], let value = schema.fields[2],
                      !key.repeated, !value.repeated, key.oneof == nil, value.oneof == nil,
                      [3, 4, 5, 6, 7, 8, 9, 13, 15, 16, 17, 18].contains(key.type) else {
                    throw ProtobufInspectionError.invalidDescriptor
                }
            }
        }
    }

    private mutating func resolveMethods(_ visibleFiles: [String: Set<String>]) throws {
        for (path, method) in methods {
            let service = "." + String(path.dropFirst().prefix { $0 != "/" })
            let input = try resolve(method.input, scope: service)
            let output = try resolve(method.output, scope: service)
            guard messages[input] != nil, messages[output] != nil,
                let owner = symbolFiles[service], let inputOwner = symbolFiles[input],
                let outputOwner = symbolFiles[output], visibleFiles[owner]?.contains(inputOwner) == true,
                visibleFiles[owner]?.contains(outputOwner) == true else {
                throw ProtobufInspectionError.invalidDescriptor
            }
            methods[path] = (input, output)
        }
    }

    private func resolve(_ name: String, scope: String) throws -> String {
        if name.hasPrefix(".") {
            guard messages[name] != nil || enums[name] != nil else { throw ProtobufInspectionError.invalidDescriptor }
            return name
        }
        var parent = scope
        while true {
            let candidate = parent + "." + name
            if messages[candidate] != nil || enums[candidate] != nil { return candidate }
            guard let dot = parent.lastIndex(of: ".") else { break }
            parent = String(parent[..<dot])
        }
        throw ProtobufInspectionError.invalidDescriptor
    }
}

private extension ProtobufSchemaBuilder {
    mutating func symbol(_ name: String, prefix: String) throws -> String {
        guard !name.isEmpty, name.utf8.enumerated().allSatisfy({ index, byte in
            (65...90).contains(byte) || (97...122).contains(byte) || byte == 95 ||
                (index > 0 && (48...57).contains(byte))
        }) else { throw ProtobufInspectionError.invalidDescriptor }
        let charge = (prefix.utf8.count + 1 + name.utf8.count) * 2
        guard budget.reserve(charge) else { throw ProtobufInspectionError.capacityExceeded }
        reserved += charge
        let full = prefix + "." + name
        guard symbols.insert(full).inserted else { throw ProtobufInspectionError.duplicateSymbol }
        symbolFiles[full] = activeFile
        return full
    }

    mutating func addEnum(_ descriptor: Google_Protobuf_EnumDescriptorProto, prefix: String) throws {
        let name = try symbol(descriptor.name, prefix: prefix)
        guard !descriptor.value.isEmpty, descriptor.unknownFields.data.isEmpty,
              descriptor.options.unknownFields.data.isEmpty, descriptor.options.uninterpretedOption.isEmpty,
            !descriptor.options.hasFeatures, descriptor.visibility == .visibilityUnset else {
            throw ProtobufInspectionError.unsupportedSchema
        }
        var values: [Int32: String] = [:]
        for value in descriptor.value {
            _ = try symbol(value.name, prefix: prefix)
            guard value.hasNumber, value.unknownFields.data.isEmpty, value.options.unknownFields.data.isEmpty,
                value.options.uninterpretedOption.isEmpty, !value.options.hasFeatures else {
                throw ProtobufInspectionError.unsupportedSchema
            }
            guard values[value.number] == nil || descriptor.options.allowAlias else {
                throw ProtobufInspectionError.invalidDescriptor
            }
            if values[value.number] == nil { values[value.number] = value.name }
        }
        enums[name] = values
    }

    mutating func addMessage(
        _ descriptor: Google_Protobuf_DescriptorProto, prefix: String, depth: Int, proto3: Bool
    ) throws {
        guard depth <= limits.depth else { throw ProtobufInspectionError.limitExceeded }
        let name = try symbol(descriptor.name, prefix: prefix)
        guard descriptor.extension.isEmpty, descriptor.extensionRange.isEmpty, descriptor.unknownFields.data.isEmpty,
              descriptor.options.unknownFields.data.isEmpty, descriptor.options.uninterpretedOption.isEmpty,
              !descriptor.options.hasFeatures, !descriptor.options.messageSetWireFormat,
              descriptor.visibility == .visibilityUnset else { throw ProtobufInspectionError.unsupportedSchema }
        if descriptor.options.mapEntry {
            guard descriptor.field.count == 2, descriptor.field.allSatisfy({
                $0.label == .optional &&
                    (($0.number == 1 && $0.name == "key") || ($0.number == 2 && $0.name == "value"))
            }) else { throw ProtobufInspectionError.invalidDescriptor }
        }
        for oneof in descriptor.oneofDecl {
            _ = try symbol(oneof.name, prefix: name)
            guard oneof.unknownFields.data.isEmpty, oneof.options.unknownFields.data.isEmpty,
                  oneof.options.uninterpretedOption.isEmpty, !oneof.options.hasFeatures else {
                throw ProtobufInspectionError.unsupportedSchema
            }
        }
        var fields: [Int: ProtobufField] = [:]
        for field in descriptor.field {
            fields[Int(field.number)] = try makeField(
                field, descriptor: descriptor, name: name, proto3: proto3, existing: fields
            )
        }
        messages[name] = ProtobufMessageSchema(fields: fields, mapEntry: descriptor.options.mapEntry)
        for value in descriptor.enumType { try addEnum(value, prefix: name) }
        for nested in descriptor.nestedType { try addMessage(nested, prefix: name, depth: depth + 1, proto3: proto3) }
    }

    mutating func makeField(
        _ field: Google_Protobuf_FieldDescriptorProto,
        descriptor: Google_Protobuf_DescriptorProto,
        name: String,
        proto3: Bool,
        existing: [Int: ProtobufField]
    ) throws -> ProtobufField {
        guard fieldCount < limits.fields else { throw ProtobufInspectionError.limitExceeded }
        fieldCount += 1
        _ = try symbol(field.name, prefix: name)
        guard field.hasNumber, field.number > 0, field.number <= 536_870_911,
            !(19_000...19_999).contains(field.number), existing[Int(field.number)] == nil,
            field.hasType, field.hasLabel, !field.hasExtendee,
            !proto3 || field.label != .required else { throw ProtobufInspectionError.invalidDescriptor }
        guard field.type != .group, field.unknownFields.data.isEmpty, field.options.unknownFields.data.isEmpty,
            field.options.uninterpretedOption.isEmpty, !field.options.hasFeatures else {
            throw ProtobufInspectionError.unsupportedSchema
        }
        if field.options.hasPacked && field.options.packed {
            guard field.label == .repeated, ![9, 10, 11, 12].contains(field.type.rawValue) else {
                throw ProtobufInspectionError.invalidDescriptor
            }
        }
        var oneof: String?
        if field.hasOneofIndex {
            guard field.oneofIndex >= 0, Int(field.oneofIndex) < descriptor.oneofDecl.count,
                field.label == .optional else { throw ProtobufInspectionError.invalidDescriptor }
            oneof = descriptor.oneofDecl[Int(field.oneofIndex)].name
        }
        guard !field.proto3Optional || (proto3 && oneof != nil) else {
            throw ProtobufInspectionError.invalidDescriptor
        }
        let typeName = field.type == .message || field.type == .enum ? field.typeName : nil
        return ProtobufField(
            name: field.name,
            number: Int(field.number),
            type: field.type.rawValue,
            typeName: typeName,
            repeated: field.label == .repeated,
            oneof: oneof
        )
    }
}

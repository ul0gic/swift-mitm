struct Arguments {
    let command: String
    let certificatePath: String
    let keyPath: String
    let value: String?
    let exportPath: String?
    let allowInternal: Bool
    let originCAPath: String?
    let editHeader: (name: String, value: String)?

    init(_ arguments: [String]) throws {
        guard arguments.count >= 3 else { throw LabError.usage }
        command = arguments[0]
        certificatePath = arguments[1]
        keyPath = arguments[2]
        if command == "serve" {
            value = nil
            exportPath = nil
            let options = try ServeOptions(arguments)
            allowInternal = options.allowInternal
            originCAPath = options.originCAPath
            editHeader = options.editHeader
        } else if command == "replay", arguments.count >= 4 {
            value = arguments[3]
            var allowInternal = false
            var originCAPath: String?
            var exportPath: String?
            var index = 4
            while index < arguments.count {
                switch arguments[index] {
                case "--allow-internal":
                    allowInternal = true
                    index += 1
                case "--origin-ca" where index + 1 < arguments.count:
                    originCAPath = arguments[index + 1]
                    index += 2
                case let path where !path.hasPrefix("--") && exportPath == nil:
                    exportPath = path
                    index += 1
                default: throw LabError.usage
                }
            }
            self.exportPath = exportPath
            self.allowInternal = allowInternal
            self.originCAPath = originCAPath
            editHeader = nil
        } else {
            throw LabError.usage
        }
    }
}

private struct ServeOptions {
    var allowInternal = false
    var originCAPath: String?
    var editHeader: (name: String, value: String)?

    init(_ arguments: [String]) throws {
        var index = 3
        while index < arguments.count {
            switch arguments[index] {
            case "--allow-internal":
                allowInternal = true
                index += 1
            case "--origin-ca" where index + 1 < arguments.count:
                originCAPath = arguments[index + 1]
                index += 2
            case "--edit-header" where index + 1 < arguments.count:
                let value = arguments[index + 1]
                guard let separator = value.firstIndex(of: "=") else { throw LabError.usage }
                editHeader = (String(value[..<separator]), String(value[value.index(after: separator)...]))
                index += 2
            default: throw LabError.usage
            }
        }
    }
}


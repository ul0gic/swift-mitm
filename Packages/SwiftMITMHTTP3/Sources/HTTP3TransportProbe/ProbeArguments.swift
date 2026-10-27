import Foundation
import NIOCore
import SwiftMITM

enum ProbeError: Error {
    case arguments
    case deniedRoute
    case missingHandler
    case capacity
    case invalidRequest
    case handshakeTimeout
}

struct ProbeArguments: Sendable {
    let certificatePath: String
    let keyPath: String
    let hostname: String
    let originAddress: SocketAddress
    let originPort: Int
    let originRootPath: String
    let port: Int
    let readyPath: String
    let allowInternal: Bool

    var authority: String { "\(Self.authorityHost(hostname)):\(originPort)" }

    private static func authorityHost(_ host: String) -> String {
        host.contains(":") && !host.hasPrefix("[") ? "[\(host)]" : host
    }

    init(_ arguments: [String]) throws {
        guard arguments.first == "serve" else { throw ProbeError.arguments }
        var values: [String: String] = [:]
        var allowInternal = false
        var index = 1
        let names: Set = [
            "--ca-cert",
            "--ca-key",
            "--hostname",
            "--origin-address",
            "--origin-port",
            "--origin-ca",
            "--port",
            "--ready-file"
        ]
        while index < arguments.count {
            let name = arguments[index]
            if name == "--allow-internal" {
                guard !allowInternal else { throw ProbeError.arguments }
                allowInternal = true
                index += 1
            } else {
                guard names.contains(name), values[name] == nil, index + 1 < arguments.count else {
                    throw ProbeError.arguments
                }
                values[name] = arguments[index + 1]
                index += 2
            }
        }
        guard let certificatePath = values["--ca-cert"], let keyPath = values["--ca-key"],
              let hostname = values["--hostname"], !hostname.isEmpty,
              let address = values["--origin-address"], let originPortValue = values["--origin-port"],
              let originPort = Int(originPortValue), (1...65535).contains(originPort),
              let originRootPath = values["--origin-ca"], let portValue = values["--port"],
              let port = Int(portValue), (0...65535).contains(port), let readyPath = values["--ready-file"] else {
            throw ProbeError.arguments
        }
        guard let identity = HTTPAuthority(authority: "\(Self.authorityHost(hostname)):\(originPort)") else {
            throw ProbeError.arguments
        }
        self.certificatePath = certificatePath
        self.keyPath = keyPath
        self.hostname = identity.host
        originAddress = try SocketAddress(ipAddress: address, port: originPort)
        self.originPort = originPort
        self.originRootPath = originRootPath
        self.port = port
        self.readyPath = readyPath
        self.allowInternal = allowInternal
    }

    func authorize() throws {
        guard !EgressPolicy(allowInternal: allowInternal).denies(originAddress) else {
            throw ProbeError.deniedRoute
        }
    }
}

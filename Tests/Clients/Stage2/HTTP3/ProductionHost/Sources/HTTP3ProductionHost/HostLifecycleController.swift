import Foundation
import SwiftMITM
import SwiftMITMHTTP3

actor HostLifecycleController {
    private struct Command: Decodable {
        let id: String
        let action: String
    }

    private struct Acknowledgement: Encodable {
        let id: String
        let action: String
        let port: Int
        let result: String
        let abandonedCallbacks: Int?
        let retainedCallbackBytes: Int?
    }

    private enum Failure: Error { case invalidCommand, unexpectedLifecycleResult }

    private let server: HTTP3ProxyServer
    private let readyPath: String
    private var port: Int
    private var lastID: String?
    private(set) var phase = "running"
    private(set) var lastStopReport: HTTPControlStopReport?

    init(server: HTTP3ProxyServer, readyPath: String, initialPort: Int) {
        self.server = server
        self.readyPath = readyPath
        port = initialPort
    }

    func poll() async throws {
        let commandPath = readyPath + ".command.json"
        guard FileManager.default.fileExists(atPath: commandPath) else { return }
        let data = try Data(contentsOf: URL(fileURLWithPath: commandPath))
        guard data.count <= 1024 else { throw Failure.invalidCommand }
        let command = try JSONDecoder().decode(Command.self, from: data)
        guard command.id != lastID else { return }
        guard (1...64).contains(command.id.utf8.count), command.id.utf8.allSatisfy({
            (48...57).contains($0) || (65...90).contains($0) || (97...122).contains($0) || $0 == 45
        }) else { throw Failure.invalidCommand }
        let snapshot = readyPath + ".lifecycle-" + command.id
        try await HostResources.write(server: server, phase: phase, path: snapshot + "-before.json")
        let result = try await perform(command.action)
        try await HostResources.write(server: server, phase: phase, path: snapshot + "-after.json")
        let acknowledgement = Acknowledgement(
            id: command.id,
            action: command.action,
            port: port,
            result: result,
            abandonedCallbacks: lastStopReport?.abandonedCallbacks,
            retainedCallbackBytes: lastStopReport?.retainedCallbackBytes
        )
        try JSONEncoder()
            .encode(acknowledgement)
            .write(to: URL(fileURLWithPath: readyPath + ".ack.json"), options: .atomic)
        lastID = command.id
    }

    private func perform(_ action: String) async throws -> String {
        switch action {
        case "stop":
            lastStopReport = try await server.stop()
            phase = "stopped"
        case "start":
            port = try await server.start(port: 0)
            phase = "running"
        case "restart":
            lastStopReport = try await server.stop()
            phase = "stopped"
            port = try await server.start(port: 0)
            phase = "running"
        case "startWhileRunning":
            do {
                _ = try await server.start(port: 0)
                throw Failure.unexpectedLifecycleResult
            } catch ProxyServerError.alreadyRunning {
                return "alreadyRunning"
            }
        case "invalidPortThenStart":
            do {
                _ = try await server.start(port: -1)
                throw Failure.unexpectedLifecycleResult
            } catch HTTP3ProxyError.invalidConfiguration {
                port = try await server.start(port: 0)
                phase = "running"
                return "invalidConfigurationThenStarted"
            }
        default: throw Failure.invalidCommand
        }
        return "success"
    }
}

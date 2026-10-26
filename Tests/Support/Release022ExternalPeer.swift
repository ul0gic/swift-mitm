import Darwin
import Foundation

struct Release022PeerReady: Decodable {
    let port: Int
    let root: String
}

enum Release022ClientError: Error {
    case missingEnvironment(String)
    case peerExited(String)
    case peerDeadline(String)
    case commandFailed(String)
}

final class Release022ExternalPeer: @unchecked Sendable {
    let directory: URL
    let python: String
    private let process = Process()
    private var output: FileHandle?
    private let readyURL: URL

    static var clientsDirectory: URL {
        URL(fileURLWithPath: #filePath)
.deletingLastPathComponent()
            .deletingLastPathComponent()
.appendingPathComponent("Clients")
    }

    init() throws {
        guard let python = ProcessInfo.processInfo.environment["SWIFTMITM_CLIENT_PYTHON"] else {
            throw Release022ClientError.missingEnvironment("SWIFTMITM_CLIENT_PYTHON")
        }
        self.python = python
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        readyURL = directory.appendingPathComponent("ready.json")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    }

    func start(script: String, arguments: [String] = []) async throws -> Release022PeerReady {
        let log = directory.appendingPathComponent("peer.log")
        _ = FileManager.default.createFile(atPath: log.path, contents: nil)
        let handle = try FileHandle(forWritingTo: log)
        output = handle
        process.environment = ProcessInfo.processInfo.environment.merging(
            ["PYTHONDONTWRITEBYTECODE": "1"]
        ) { _, new in new }
        process.executableURL = URL(fileURLWithPath: python)
        process.arguments = [Self.clientsDirectory.appendingPathComponent(script).path]
            + arguments + ["--ready-file", readyURL.path]
        process.standardOutput = handle
        process.standardError = handle
        try process.run()
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(15))
        while !FileManager.default.fileExists(atPath: readyURL.path) {
            guard process.isRunning else {
                throw Release022ClientError.peerExited(try String(contentsOf: log, encoding: .utf8))
            }
            guard clock.now < deadline else {
                throw Release022ClientError.peerDeadline(try String(contentsOf: log, encoding: .utf8))
            }
            try await Task.sleep(for: .milliseconds(20))
        }
        return try JSONDecoder().decode(Release022PeerReady.self, from: Data(contentsOf: readyURL))
    }

    func stop() {
        if process.isRunning {
            process.terminate()
            let deadline = Date().addingTimeInterval(3)
            while process.isRunning, Date() < deadline { Thread.sleep(forTimeInterval: 0.01) }
            if process.isRunning {
                kill(process.processIdentifier, SIGKILL)
            }
            process.waitUntilExit()
        }
        try? output?.close()
        try? FileManager.default.removeItem(at: directory)
    }
}

func release022RunProcess(
    executable: String,
    arguments: [String],
    environment: [String: String]? = nil
) throws -> String {
    let process = Process()
    let output = Pipe()
    let error = Pipe()
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = arguments
    process.environment = environment
    process.standardOutput = output
    process.standardError = error
    try process.run()
    process.waitUntilExit()
    let result = String(decoding: output.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
    let diagnostic = String(decoding: error.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
    guard process.terminationStatus == 0 else { throw Release022ClientError.commandFailed(diagnostic) }
    return result
}

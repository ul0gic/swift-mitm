import Darwin
import Foundation
import XCTest

struct Release022PeerReady: Decodable {
    let port: Int
    let root: String
}

enum Release022ClientError: Error {
    case missingEnvironment(String)
    case peerExited(String)
    case peerDeadline(String)
    case commandFailed(String)
    case commandDeadline(phase: String, diagnostic: String)
    case commandCleanupFailed(phase: String)
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
    environment: [String: String]? = nil,
    timeout: TimeInterval = 120,
    phase: String = "command"
) throws -> String {
    let process = Process()
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer {
        do { try FileManager.default.removeItem(at: directory) } catch {
            XCTFail("Command output cleanup failed: \(error)")
        }
    }
    let outputURL = directory.appendingPathComponent("stdout")
    let errorURL = directory.appendingPathComponent("stderr")
    try Data().write(to: outputURL)
    try Data().write(to: errorURL)
    let output = try FileHandle(forWritingTo: outputURL)
    defer {
        do { try output.close() } catch { XCTFail("Command stdout close failed: \(error)") }
    }
    let errorOutput = try FileHandle(forWritingTo: errorURL)
    defer {
        do { try errorOutput.close() } catch { XCTFail("Command stderr close failed: \(error)") }
    }
    process.executableURL = URL(fileURLWithPath: executable)
    process.arguments = arguments
    process.environment = environment
    process.standardOutput = output
    process.standardError = errorOutput
    let completion = DispatchSemaphore(value: 0)
    process.terminationHandler = { _ in completion.signal() }
    try process.run()
    let expired = try release022AwaitCommand(process, completion: completion, timeout: timeout, phase: phase)
    let diagnostic = try release022CommandDiagnostic(errorURL)
    guard !expired else { throw Release022ClientError.commandDeadline(phase: phase, diagnostic: diagnostic) }
    guard process.terminationStatus == 0 else {
        throw Release022ClientError.commandFailed("\(phase): \(diagnostic)")
    }
    return try String(contentsOf: outputURL, encoding: .utf8)
}

private func release022CommandDiagnostic(_ url: URL) throws -> String {
    let handle = try FileHandle(forReadingFrom: url)
    defer {
        do { try handle.close() } catch { XCTFail("Command diagnostic close failed: \(error)") }
    }
    let size = try handle.seekToEnd()
    guard size > 0 else { return "" }
    try handle.seek(toOffset: size > 8192 ? size - 8192 : 0)
    guard let bytes = try handle.read(upToCount: 8192) else {
        throw Release022ClientError.commandFailed("stderr ended before its recorded tail could be read")
    }
    if let text = String(data: bytes, encoding: .utf8) {
        return text
    }
    return "stderr tail contains incomplete or invalid UTF-8; base64=\(bytes.base64EncodedString())"
}

private func release022AwaitCommand(
    _ process: Process,
    completion: DispatchSemaphore,
    timeout: TimeInterval,
    phase: String
) throws -> Bool {
    let identifier = process.processIdentifier
    let ownsGroup = getpgid(identifier) == identifier && identifier != getpgrp()
    guard completion.wait(timeout: .now() + timeout) == .timedOut else { return false }
    process.terminate()
    if completion.wait(timeout: .now() + 3) == .timedOut {
        guard process.isRunning else { throw Release022ClientError.commandCleanupFailed(phase: phase) }
        let target = ownsGroup && getpgid(identifier) == identifier ? -identifier : identifier
        guard kill(target, SIGKILL) == 0 || errno == ESRCH, completion.wait(timeout: .now() + 3) == .success else {
            throw Release022ClientError.commandCleanupFailed(phase: phase)
        }
    }
    if ownsGroup, kill(-identifier, 0) == 0 {
        throw Release022ClientError.commandCleanupFailed(phase: phase)
    }
    return true
}

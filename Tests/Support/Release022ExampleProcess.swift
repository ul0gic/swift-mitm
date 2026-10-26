import Darwin
import Foundation

import SwiftMITM

final class Release022ExampleProcess: @unchecked Sendable {
    let directory: URL
    private let process = Process()
    private let output = Pipe()
    private let diagnostic = Pipe()
    private let condition = NSCondition()
    private var bytes = Data()
    private var errorBytes = Data()
    private var outputClosed = false
    private var diagnosticEnded = false

    init() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    }

    func start(
        root: GeneratedAuthority, originRoot: String, asynchronous: Bool, captureBytes: Int
    ) throws -> Int {
        guard let executable = ProcessInfo.processInfo.environment["SWIFTMITM_FIRST_CAPTURE"] else {
            throw Release022ClientError.missingEnvironment("SWIFTMITM_FIRST_CAPTURE")
        }
        let certificate = directory.appendingPathComponent("proxy-ca.pem")
        let key = directory.appendingPathComponent("proxy-key.pem")
        let upstream = directory.appendingPathComponent("upstream-ca.pem")
        try Data(root.certificatePEM.utf8).write(to: certificate)
        try Data(root.privateKeyPEM.utf8).write(to: key, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: key.path)
        try Data(originRoot.utf8).write(to: upstream)
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = ["--ca-cert", certificate.path, "--ca-key", key.path, "--port", "0",
                             "--allow-internal", "--upstream-ca", upstream.path,
                             "--capture-bytes", String(captureBytes)] + (asynchronous ? ["--async"] : [])
        process.standardOutput = output
        process.standardError = diagnostic
        diagnostic.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let incoming = handle.availableData
            guard !incoming.isEmpty else {
                handle.readabilityHandler = nil
                self?.condition.withLock {
                    self?.diagnosticEnded = true
                    self?.condition.broadcast()
                }
                return
            }
            self?.condition.withLock {
                self?.errorBytes.append(incoming)
                self?.condition.broadcast()
            }
        }
        resumeOutput()
        try process.run()
        let records = try wait { $0.contains { $0["type"] as? String == "listening" } }
        guard let port = records.first(where: { $0["type"] as? String == "listening" })?["port"] as? Int else {
            throw Phase2FixtureError.unexpectedBytes
        }
        return port
    }

    func pauseOutput() { output.fileHandleForReading.readabilityHandler = nil }

    func closeOutputReader() throws {
        pauseOutput()
        try output.fileHandleForReading.close()
        outputClosed = true
    }

    func resumeOutput() {
        guard !outputClosed else { return }
        output.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let incoming = handle.availableData
            guard !incoming.isEmpty else { handle.readabilityHandler = nil; return }
            self?.condition.withLock {
                self?.bytes.append(incoming)
                self?.condition.broadcast()
            }
        }
    }

    func wait(until predicate: ([[String: Any]]) -> Bool) throws -> [[String: Any]] {
        let deadline = Date().addingTimeInterval(15)
        return try condition.withLock {
            while true {
                let records = try Self.records(bytes, stream: "stdout") + Self.records(errorBytes, stream: "stderr")
                if predicate(records) { return records }
                guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
            }
        }
    }

    func requestStop(signal: Int32 = SIGTERM) {
        if process.isRunning { kill(process.processIdentifier, signal) }
    }

    func waitForExit(timeout: TimeInterval) throws -> Int32 {
        let deadline = Date().addingTimeInterval(timeout)
        while process.isRunning {
            guard Date() < deadline else { throw Phase2FixtureError.deadlineExceeded }
            Thread.sleep(forTimeInterval: 0.01)
        }
        process.waitUntilExit()
        try condition.withLock {
            while !diagnosticEnded {
                guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
            }
        }
        return process.terminationStatus
    }

    private static func records(_ data: Data, stream: String) throws -> [[String: Any]] {
        try data.split(separator: 10).compactMap { line -> [String: Any]? in
            guard line.first == 123, line.last == 125 else { return nil }
            guard var record = try JSONSerialization.jsonObject(with: Data(line)) as? [String: Any] else { return nil }
            record["_stream"] = stream
            return record
        }
    }

    func stop() {
        resumeOutput()
        requestStop()
        let deadline = Date().addingTimeInterval(3)
        while process.isRunning, Date() < deadline { Thread.sleep(forTimeInterval: 0.01) }
        if process.isRunning { kill(process.processIdentifier, SIGKILL) }
        if process.processIdentifier != 0 { process.waitUntilExit() }
        output.fileHandleForReading.readabilityHandler = nil
        diagnostic.fileHandleForReading.readabilityHandler = nil
        if !outputClosed { try? output.fileHandleForReading.close() }
        try? diagnostic.fileHandleForReading.close()
        try? FileManager.default.removeItem(at: directory)
    }
}

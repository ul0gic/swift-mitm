import Darwin
import Foundation
import XCTest

private struct Release022TrustRemovalResult: Decodable {
    let schema: String
    let status: String
}

func release022RemoveRunnerTrust(certificate: URL, fingerprint: String) throws {
    let environment = ProcessInfo.processInfo.environment
    guard environment["GITHUB_ACTIONS"] == "true", environment["RUNNER_ENVIRONMENT"] == "github-hosted" else {
        throw Release022ClientError.missingEnvironment("ephemeral GitHub-hosted runner required for trust removal")
    }
    guard let python = environment["SWIFTMITM_CLIENT_PYTHON"] else {
        throw Release022ClientError.missingEnvironment("SWIFTMITM_CLIENT_PYTHON")
    }
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(
        at: directory,
        withIntermediateDirectories: false,
        attributes: [.posixPermissions: 0o700]
    )
    defer {
        do { try FileManager.default.removeItem(at: directory) } catch {
            XCTFail("Trust removal diagnostic cleanup failed: \(error)")
        }
    }
    let report = directory.appendingPathComponent("report.jsonl")
    let errors = directory.appendingPathComponent("stderr")
    let output = try release022TrustDiagnosticOutput(report)
    defer { release022CloseTrustDiagnostic(output) }
    let errorOutput = try release022TrustDiagnosticOutput(errors)
    defer { release022CloseTrustDiagnostic(errorOutput) }
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/sudo")
    process.standardOutput = output
    process.standardError = errorOutput
    let helper = Release022ExternalPeer.clientsDirectory.appendingPathComponent("runner_trust_removal.py")
    let arguments = [
        "-n",
        python,
        "-I",
        "-S",
        helper.path,
        "--certificate",
        certificate.path,
        "--sha256",
        fingerprint,
        "--started-nanoseconds"
    ]
    try release022AwaitTrustRemoval(process, arguments: arguments, report: report, errors: errors)
}

private func release022TrustDiagnosticOutput(_ path: URL) throws -> FileHandle {
    guard FileManager.default.createFile(
        atPath: path.path,
        contents: Data(),
        attributes: [.posixPermissions: 0o600]
    ) else {
        throw Release022ClientError.commandFailed("could not create private trust removal diagnostic")
    }
    return try FileHandle(forWritingTo: path)
}

private func release022AwaitTrustRemoval(_ process: Process, arguments: [String], report: URL, errors: URL) throws {
    let completion = DispatchSemaphore(value: 0)
    process.terminationHandler = { _ in completion.signal() }
    let started = try release022MonotonicNanoseconds()
    process.arguments = arguments + [String(started)]
    try process.run()
    let elapsed = try release022MonotonicNanoseconds() - started
    let remaining = elapsed < 30_000_000_000 ? 30_000_000_000 - elapsed : 0
    let completed = completion.wait(timeout: .now() + .nanoseconds(Int(remaining))) == .success
    guard completed else {
        process.terminate()
        do { _ = try release022EmitTrustDiagnostic(report: report, errors: errors) } catch {
            XCTFail("Trust removal diagnostic unavailable after deadline: \(error)")
        }
        throw Release022ClientError.commandDeadline(
            phase: "security-admin remove-trusted-cert",
            diagnostic: "supervisor completion unconfirmed; wrapperPID=\(process.processIdentifier)"
        )
    }
    let data = try release022EmitTrustDiagnostic(report: report, errors: errors)
    guard let finalLine = data.split(separator: 0x0A).last else {
        throw Release022ClientError.commandFailed("trust removal supervisor omitted its result")
    }
    let result = try JSONDecoder().decode(Release022TrustRemovalResult.self, from: Data(finalLine))
    guard result.schema == "SwiftMITM.runnerTrustRemoval.v1" else {
        throw Release022ClientError.commandFailed("unexpected trust removal diagnostic schema")
    }
    if result.status == "deadline" {
        throw Release022ClientError.commandDeadline(
            phase: "security-admin remove-trusted-cert",
            diagnostic: "see bounded report"
        )
    }
    if result.status == "cleanupFailure" {
        throw Release022ClientError.commandCleanupFailed(phase: "security-admin remove-trusted-cert supervisor")
    }
    guard process.terminationStatus == 0, result.status == "removed" else {
        throw Release022ClientError.commandFailed("security-admin remove-trusted-cert: see bounded supervisor report")
    }
}

private func release022EmitTrustDiagnostic(report: URL, errors: URL) throws -> Data {
    let (data, truncated) = try release022ReadTrustDiagnostic(report, maximum: 63_488)
    let (errorData, errorTruncated) = try release022ReadTrustDiagnostic(errors, maximum: 1024)
    FileHandle.standardError.write(Data("Chromium trust removal diagnostic (truncated=\(truncated)):\n".utf8))
    FileHandle.standardError.write(data)
    if !errorData.isEmpty {
        let label = "Chromium trust removal supervisor stderr (truncated=\(errorTruncated)):\n"
        FileHandle.standardError.write(Data(label.utf8))
        FileHandle.standardError.write(errorData)
    }
    guard !truncated else {
        throw Release022ClientError.commandFailed("trust removal diagnostic exceeded its output bound")
    }
    return data
}

private func release022MonotonicNanoseconds() throws -> UInt64 {
    var value = timespec()
    guard clock_gettime(CLOCK_MONOTONIC, &value) == 0,
          value.tv_sec >= 0, value.tv_nsec >= 0 else {
        throw Release022ClientError.commandFailed("CLOCK_MONOTONIC read failed")
    }
    return UInt64(value.tv_sec) * 1_000_000_000 + UInt64(value.tv_nsec)
}

private func release022ReadTrustDiagnostic(_ path: URL, maximum: Int) throws -> (Data, Bool) {
    let input = try FileHandle(forReadingFrom: path)
    defer { release022CloseTrustDiagnostic(input) }
    let size = try input.seekToEnd()
    try input.seek(toOffset: 0)
    if size == 0 {
        return (Data(), false)
    }
    guard let bytes = try input.read(upToCount: maximum) else {
        throw Release022ClientError.commandFailed("trust removal diagnostic ended before its recorded size")
    }
    return (bytes, size > UInt64(maximum))
}

private func release022CloseTrustDiagnostic(_ handle: FileHandle) {
    do { try handle.close() } catch { XCTFail("Trust removal diagnostic handle close failed: \(error)") }
}

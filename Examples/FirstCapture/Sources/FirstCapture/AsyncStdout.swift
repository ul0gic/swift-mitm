import Darwin
import Dispatch
import Foundation
import os

enum OutputError: Error {
    case configuration(Int32)
    case write(Int32)
}

final class AsyncStdout: Sendable {
    private static let newline = Data([10])

    private struct DescriptorSettings: Sendable {
        let descriptor: Int32
        let flags: Int32
        let noSignalPipe: Int32

        init(_ descriptor: Int32) throws {
            self.descriptor = descriptor
            flags = fcntl(descriptor, F_GETFL)
            guard flags >= 0 else { throw OutputError.configuration(errno) }
            noSignalPipe = fcntl(descriptor, F_GETNOSIGPIPE)
            guard noSignalPipe >= 0 else { throw OutputError.configuration(errno) }
        }

        func configure() throws {
            guard fcntl(descriptor, F_SETNOSIGPIPE, 1) == 0, fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0 else {
                throw OutputError.configuration(errno)
            }
        }

        func restore() throws {
            let flagResult = fcntl(descriptor, F_SETFL, flags)
            let flagError = errno
            let signalResult = fcntl(descriptor, F_SETNOSIGPIPE, noSignalPipe)
            if flagResult < 0 {
                throw OutputError.configuration(flagError)
            }
            if signalResult < 0 {
                throw OutputError.configuration(errno)
            }
        }
    }

    private struct State: Sendable {
        var pending: Data?
        var offset = 0
        var waiter: CheckedContinuation<Void, any Error>?
        var failure: DeliveryError?
        var active = false
        var draining = false
        var closed = false
        var terminalReported = false
    }

    private let state = OSAllocatedUnfairLock(initialState: State())
    private let source: any DispatchSourceWrite
    private let stdoutSettings: DescriptorSettings
    private let stderrSettings: DescriptorSettings

    init() throws {
        stdoutSettings = try DescriptorSettings(STDOUT_FILENO)
        stderrSettings = try DescriptorSettings(STDERR_FILENO)
        do {
            try stdoutSettings.configure()
            try stderrSettings.configure()
        } catch {
            try Self.restore(stdoutSettings, stderrSettings)
            throw error
        }
        source = DispatchSource.makeWriteSource(
            fileDescriptor: STDOUT_FILENO,
            queue: DispatchQueue(label: "FirstCapture.stdout")
        )
        source.setEventHandler { [weak self] in self?.writeReady() }
    }

    deinit {
        state.withLock { state in
            if !state.closed {
                if !state.active {
                    source.resume()
                }
                source.cancel()
            }
        }
    }

    func line(_ data: Data) async throws {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                state.withLock { state in
                    if let failure = state.failure {
                        continuation.resume(throwing: failure)
                        return
                    }
                    state.pending = data
                    state.offset = 0
                    state.waiter = continuation
                    state.active = true
                    source.resume()
                }
                writeReady()
            }
        } onCancel: {
            self.cancel(.cancelled)
        }
    }

    func beginDrain() {
        state.withLock { $0.draining = true }
        writeReady()
    }

    func cancel(_ failure: DeliveryError) {
        state.withLock { state in
            guard state.failure == nil else { return }
            state.failure = failure
            state.pending = nil
            state.waiter?.resume(throwing: failure)
            state.waiter = nil
            if !state.active {
                source.resume()
            }
            state.active = false
            source.cancel()
            state.closed = true
        }
    }

    func reportLoss(_ failure: DeliveryError) {
        cancel(failure)
        state.withLock { state in
            guard !state.terminalReported else { return }
            state.terminalReported = true
            let record = Data((
                "{\"type\":\"deliveryLoss\",\"reason\":\"\(failure.rawValue)\","
                    + "\"allActiveCapturesIncomplete\":true}\n"
            ).utf8)
            if state.offset == 0 {
                Self.bestEffort(record, descriptor: STDOUT_FILENO)
            }
            Self.bestEffort(record, descriptor: STDERR_FILENO)
        }
    }

    func restoreFlags() throws {
        state.withLock { state in
            if state.failure == nil {
                state.failure = .cancelled
                if !state.active {
                    source.resume()
                }
                source.cancel()
                state.closed = true
                state.active = false
            }
        }
        try Self.restore(stdoutSettings, stderrSettings)
    }

    private static func restore(_ stdout: DescriptorSettings, _ stderr: DescriptorSettings) throws {
        let stdoutResult = Result { try stdout.restore() }
        let stderrResult = Result { try stderr.restore() }
        try stdoutResult.get()
        try stderrResult.get()
    }

    private func writeReady() {
        let cancelDrain = state.withLock { state -> Bool in
            guard let data = state.pending, state.failure == nil else { return false }
            repeat {
                let result = writePart(data, offset: state.offset)
                if result > 0 {
                    state.offset += result
                    if state.offset == data.count + 1 {
                        complete(&state, result: .success(()))
                        return false
                    }
                } else if result < 0, errno == EAGAIN || errno == EWOULDBLOCK {
                    return state.draining
                } else if result < 0, errno != EINTR {
                    complete(&state, result: .failure(OutputError.write(errno)))
                    return false
                }
            } while state.draining
            return false
        }
        if cancelDrain {
            cancel(.cancelled)
        }
    }

    private func writePart(_ data: Data, offset: Int) -> Int {
        if offset == data.count {
            return Self.newline.withUnsafeBytes { buffer in
                Darwin.write(STDOUT_FILENO, buffer.baseAddress, buffer.count)
            }
        }
        return data.withUnsafeBytes { buffer in
            Darwin.write(STDOUT_FILENO, buffer.baseAddress?.advanced(by: offset), data.count - offset)
        }
    }

    private func complete(_ state: inout State, result: Result<Void, any Error>) {
        state.pending = nil
        state.offset = 0
        state.waiter?.resume(with: result)
        state.waiter = nil
        state.active = false
        source.suspend()
    }

    private static func bestEffort(_ data: Data, descriptor: Int32) {
        data.withUnsafeBytes { buffer in
            _ = Darwin.write(descriptor, buffer.baseAddress, buffer.count)
        }
    }
}

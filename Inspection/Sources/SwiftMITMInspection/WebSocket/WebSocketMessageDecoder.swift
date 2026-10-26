import Foundation
import SwiftMITM

public enum WebSocketMessageError: Error, Sendable {
    case invalidConfiguration
    case unsupportedExtension
    case invalidSequence
    case messageLimit
    case malformedCompression
    case invalidUTF8
    case terminated
}

public struct WebSocketMessageSummary: Sendable {
    public let opcode: WebSocketOpcode
    public let compressed: Bool
    public let encodedBytes: Int
    public let decodedBytes: Int
}

public struct WebSocketMessageProgress: Sendable {
    public let consumedBytes: Int
    public let producedBytes: Int
    public let outputLimit: Bool
    public let message: WebSocketMessageSummary?
}

public final class WebSocketMessageDecoder {
    private let maximum: Int
    private let budget: InspectionMemoryBudget
    private let noContextTakeover: Bool
    private let windowBits: Int32?
    private var inflater: WebSocketInflater?
    private var opcode: WebSocketOpcode?
    private var compressed = false
    private var frameOpen = false
    private var control = false
    private var final = false
    private var remaining = 0
    private var encoded = 0
    private var decoded = 0
    private var utf8Remaining = 0
    private var utf8Lower: UInt8 = 0x80
    private var utf8Upper: UInt8 = 0xBF
    private var failed = false
    private var tailOffset = 0

    public init(
        direction: WebSocketDirection,
        negotiatedExtensions: [String],
        budget: InspectionMemoryBudget,
        maximumMessageBytes: Int = 4 * 1024 * 1024
    ) throws {
        guard (1...(16 * 1024 * 1024)).contains(maximumMessageBytes) else {
            throw WebSocketMessageError.invalidConfiguration
        }
        self.maximum = maximumMessageBytes
        self.budget = budget
        let prefix = direction == .clientToServer ? "client" : "server"
        let negotiation = try Self.negotiate(negotiatedExtensions, prefix: prefix)
        windowBits = negotiation.window
        noContextTakeover = negotiation.noContext
    }

    private static func negotiate(
        _ headers: [String], prefix: String
    ) throws -> (window: Int32?, noContext: Bool) {
        var selectedWindow: Int32?
        var selectedNoContext = false
        var found = false
        for header in headers {
            for item in header.split(separator: ",", omittingEmptySubsequences: false) {
                let parts = item.split(separator: ";", omittingEmptySubsequences: false)
                    .map { $0.trimmingCharacters(in: .whitespaces) }
                guard parts.first?.lowercased() == "permessage-deflate", !found else {
                    throw WebSocketMessageError.unsupportedExtension
                }
                found = true
                selectedWindow = 15
                var seen = Set<String>()
                for parameter in parts.dropFirst() {
                    let (name, value) = try Self.parameter(parameter)
                    guard seen.insert(name).inserted else { throw WebSocketMessageError.unsupportedExtension }
                    if name == prefix + "_no_context_takeover" {
                        selectedNoContext = true
                    }
                    if name == prefix + "_max_window_bits" {
                        selectedWindow = value
                    }
                }
            }
        }
        return (selectedWindow, selectedNoContext)
    }

    private static func parameter(_ parameter: String) throws -> (name: String, value: Int32?) {
        let pair = parameter.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
        let name = pair[0].trimmingCharacters(in: .whitespaces).lowercased()
        switch name {
        case "client_no_context_takeover", "server_no_context_takeover":
            guard pair.count == 1 else { throw WebSocketMessageError.unsupportedExtension }
            return (name, nil)
        case "client_max_window_bits", "server_max_window_bits":
            guard pair.count == 2,
                let value = Int32(pair[1].trimmingCharacters(in: .whitespaces)),
                (8...15).contains(value)
            else { throw WebSocketMessageError.unsupportedExtension }
            return (name, value)
        default: throw WebSocketMessageError.unsupportedExtension
        }
    }

    public func startFrame(opcode frameOpcode: WebSocketOpcode, fin: Bool, compressed: Bool, byteCount: Int) throws {
        guard !failed else { throw WebSocketMessageError.terminated }
        do {
            guard !frameOpen, byteCount >= 0 else { throw WebSocketMessageError.invalidSequence }
            control = frameOpcode == .ping || frameOpcode == .pong || frameOpcode == .connectionClose
            if control {
                guard fin, !compressed, byteCount <= 125 else { throw WebSocketMessageError.invalidSequence }
            } else {
                if frameOpcode == .continuation {
                    guard opcode != nil, !compressed else { throw WebSocketMessageError.invalidSequence }
                } else {
                    try startMessage(opcode: frameOpcode, compressed: compressed)
                }
                guard byteCount <= maximum - encoded else { throw WebSocketMessageError.messageLimit }
                encoded += byteCount
            }
            frameOpen = true
            remaining = byteCount
            final = fin
            tailOffset = 0
        } catch {
            terminate()
            throw error
        }
    }

    private func startMessage(opcode frameOpcode: WebSocketOpcode, compressed: Bool) throws {
        guard opcode == nil, frameOpcode == .text || frameOpcode == .binary else {
            throw WebSocketMessageError.invalidSequence
        }
        opcode = frameOpcode
        self.compressed = compressed
        encoded = 0
        decoded = 0
        utf8Remaining = 0
        if compressed {
            guard let windowBits else { throw WebSocketMessageError.unsupportedExtension }
            if inflater == nil {
                inflater = try WebSocketInflater(windowBits: windowBits, budget: budget)
            }
        }
    }

    public func feed(
        _ bytes: UnsafeRawBufferPointer,
        maximumOutputBytes: Int = 1_048_576,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws -> WebSocketMessageProgress {
        guard !failed else { throw WebSocketMessageError.terminated }
        do {
            guard frameOpen, bytes.count <= remaining, (1...1_048_576).contains(maximumOutputBytes)
            else { throw WebSocketMessageError.invalidSequence }
            if control {
                remaining -= bytes.count
                return WebSocketMessageProgress(
                    consumedBytes: bytes.count, producedBytes: 0, outputLimit: false, message: nil
                )
            }
            if compressed {
                guard let inflater else { throw WebSocketMessageError.invalidSequence }
                let progress = try inflater.feed(bytes, maximumOutputBytes: maximumOutputBytes) {
                    try self.output($0, emit: emit)
                }
                remaining -= progress.consumed
                return WebSocketMessageProgress(
                    consumedBytes: progress.consumed,
                    producedBytes: progress.produced,
                    outputLimit: progress.outputLimit,
                    message: nil
                )
            }
            let count = min(bytes.count, maximumOutputBytes)
            try output(UnsafeRawBufferPointer(rebasing: bytes[..<count]), emit: emit)
            remaining -= count
            return WebSocketMessageProgress(
                consumedBytes: count, producedBytes: count, outputLimit: count == maximumOutputBytes, message: nil
            )
        } catch {
            terminate()
            throw error
        }
    }

    public func endFrame(
        maximumOutputBytes: Int = 1_048_576,
        emit: (UnsafeRawBufferPointer) throws -> Void
    ) throws -> WebSocketMessageProgress {
        guard !failed else { throw WebSocketMessageError.terminated }
        do {
            guard frameOpen, remaining == 0, (1...1_048_576).contains(maximumOutputBytes)
            else { throw WebSocketMessageError.invalidSequence }
            guard !control, final else {
                frameOpen = false
                return WebSocketMessageProgress(consumedBytes: 0, producedBytes: 0, outputLimit: false, message: nil)
            }
            var produced = 0
            if compressed, let inflater {
                let progress = try [UInt8(0), 0, 255, 255].withUnsafeBytes { tail in
                    try inflater.feed(
                        UnsafeRawBufferPointer(rebasing: tail[tailOffset...]),
                        maximumOutputBytes: maximumOutputBytes
                    ) { try self.output($0, emit: emit) }
                }
                tailOffset += progress.consumed
                produced = progress.produced
                if progress.outputLimit {
                    return WebSocketMessageProgress(
                        consumedBytes: 0, producedBytes: produced, outputLimit: true, message: nil
                    )
                }
                guard inflater.atBoundary else { throw WebSocketMessageError.malformedCompression }
                if noContextTakeover {
                    self.inflater = nil
                }
            }
            guard utf8Remaining == 0, let opcode else { throw WebSocketMessageError.invalidUTF8 }
            let result = WebSocketMessageSummary(
                opcode: opcode, compressed: compressed, encodedBytes: encoded, decodedBytes: decoded
            )
            self.opcode = nil
            frameOpen = false
            return WebSocketMessageProgress(
                consumedBytes: 0, producedBytes: produced, outputLimit: false, message: result
            )
        } catch {
            terminate()
            throw error
        }
    }

    public func terminate() {
        failed = true
        inflater = nil
    }
}

private extension WebSocketMessageDecoder {
    func output(_ bytes: UnsafeRawBufferPointer, emit: (UnsafeRawBufferPointer) throws -> Void) throws {
        guard bytes.count <= maximum - decoded else { throw WebSocketMessageError.messageLimit }
        if opcode == .text {
            try validateUTF8(bytes)
        }
        decoded += bytes.count
        var offset = 0
        while offset < bytes.count {
            let end = min(offset + 65_536, bytes.count)
            try emit(UnsafeRawBufferPointer(rebasing: bytes[offset..<end]))
            offset = end
        }
    }

    func validateUTF8(_ bytes: UnsafeRawBufferPointer) throws {
        for byte in bytes {
            if utf8Remaining > 0 {
                guard byte >= utf8Lower, byte <= utf8Upper else { throw WebSocketMessageError.invalidUTF8 }
                utf8Remaining -= 1
                utf8Lower = 0x80
                utf8Upper = 0xBF
            } else {
                try beginScalar(byte)
            }
        }
    }

    func beginScalar(_ byte: UInt8) throws {
        switch byte {
        case 0...0x7F: break
        case 0xC2...0xDF: utf8Remaining = 1
        case 0xE0:
            utf8Remaining = 2
            utf8Lower = 0xA0
        case 0xE1...0xEC, 0xEE...0xEF: utf8Remaining = 2
        case 0xED:
            utf8Remaining = 2
            utf8Upper = 0x9F
        case 0xF0:
            utf8Remaining = 3
            utf8Lower = 0x90
        case 0xF1...0xF3: utf8Remaining = 3
        case 0xF4:
            utf8Remaining = 3
            utf8Upper = 0x8F
        default: throw WebSocketMessageError.invalidUTF8
        }
    }
}

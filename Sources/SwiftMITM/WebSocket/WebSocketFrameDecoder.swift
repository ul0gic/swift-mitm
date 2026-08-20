import Foundation

final class WebSocketFrameDecoder {
    struct Frame {
        let opcode: WebSocketOpcode
        let fin: Bool
        let compressed: Bool
        let bytes: [UInt8]
        let byteCount: Int
        let truncated: Bool
        let closeCode: Int?
        let closeReason: String?
    }

    private enum Phase {
        case header
        case payload
        case failed
    }

    private let captureLimit: Int
    private var phase: Phase = .header
    private var header: [UInt8] = []
    private var opcode: WebSocketOpcode = .binary
    private var fin = false
    private var compressed = false
    private var masked = false
    private var maskKey: [UInt8] = []
    private var maskIndex = 0
    private var remaining = 0
    private var total = 0
    private var captured: [UInt8] = []

    init(captureLimit: Int) {
        self.captureLimit = max(0, captureLimit)
    }

    var isAtFrameBoundary: Bool {
        phase == .header && header.isEmpty
    }

    @discardableResult
    func decode<Bytes: Sequence>(_ bytes: Bytes, emit: (Frame) -> Void) -> Bool where Bytes.Element == UInt8 {
        for byte in bytes {
            switch phase {
            case .header:
                consumeHeaderByte(byte, emit: emit)
            case .payload:
                consumePayloadByte(byte, emit: emit)
            case .failed:
                return false
            }
        }
        return phase != .failed
    }

    private func consumeHeaderByte(_ byte: UInt8, emit: (Frame) -> Void) {
        header.append(byte)
        guard header.count >= requiredHeaderBytes else { return }
        guard finalizeHeader() else {
            phase = .failed
            return
        }
        if remaining == 0 {
            emit(makeFrame())
            resetForNextFrame()
        }
    }

    private var requiredHeaderBytes: Int {
        guard header.count >= 2 else { return 2 }
        let masked = header[1] & 0x80 != 0
        let lengthCode = header[1] & 0x7F
        var count = 2
        if lengthCode == 126 {
            count += 2
        } else if lengthCode == 127 {
            count += 8
        }
        if masked {
            count += 4
        }
        return count
    }

    private func finalizeHeader() -> Bool {
        let byte0 = header[0]
        fin = byte0 & 0x80 != 0
        compressed = byte0 & 0x40 != 0
        guard byte0 & 0x30 == 0, let code = WebSocketOpcode(rawValue: byte0 & 0x0F) else { return false }
        opcode = code
        masked = header[1] & 0x80 != 0
        let lengthCode = header[1] & 0x7F
        if opcode == .continuation, compressed {
            return false
        }
        if isControlFrame, !fin || compressed || lengthCode > 125 {
            return false
        }
        var cursor = 2
        let length: Int
        if lengthCode == 126 {
            length = Int(header[2]) << 8 | Int(header[3])
            guard length >= 126 else { return false }
            cursor = 4
        } else if lengthCode == 127 {
            guard header[2] & 0x80 == 0 else { return false }
            var value: UInt64 = 0
            for offset in 0..<8 {
                value = value << 8 | UInt64(header[2 + offset])
            }
            guard value >= 65_536, value <= UInt64(Int.max) else { return false }
            length = Int(value)
            cursor = 10
        } else {
            length = Int(lengthCode)
        }
        if masked {
            maskKey = Array(header[cursor..<(cursor + 4)])
        } else {
            maskKey = []
        }
        remaining = length
        total = length
        maskIndex = 0
        captured = []
        phase = .payload
        return true
    }

    private var isControlFrame: Bool {
        opcode == .connectionClose || opcode == .ping || opcode == .pong
    }

    private func consumePayloadByte(_ byte: UInt8, emit: (Frame) -> Void) {
        let unmasked = masked ? byte ^ maskKey[maskIndex & 3] : byte
        maskIndex += 1
        if captured.count < captureLimit {
            captured.append(unmasked)
        }
        remaining -= 1
        guard remaining == 0 else { return }
        emit(makeFrame())
        resetForNextFrame()
    }

    private func resetForNextFrame() {
        phase = .header
        header.removeAll(keepingCapacity: true)
        maskKey.removeAll(keepingCapacity: true)
    }

    private func makeFrame() -> Frame {
        var closeCode: Int?
        var closeReason: String?
        if opcode == .connectionClose, captured.count >= 2 {
            closeCode = Int(captured[0]) << 8 | Int(captured[1])
            closeReason = String(bytes: captured.dropFirst(2), encoding: .utf8)
        }
        return Frame(
            opcode: opcode,
            fin: fin,
            compressed: compressed,
            bytes: captured,
            byteCount: total,
            truncated: total > captured.count,
            closeCode: closeCode,
            closeReason: closeReason
        )
    }
}

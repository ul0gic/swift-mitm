import Foundation

/// Streaming RFC 6455 frame decoder for the post-handshake tunnel. Observational: it unmasks and bounds the
/// captured payload but never buffers a whole large frame. Malformed input parks the decoder (capture stops,
/// forwarding is unaffected — the tap forwards bytes regardless of decode state).
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
        self.captureLimit = captureLimit > 0 ? captureLimit : 1_048_576
    }

    func decode<Bytes: Sequence>(_ bytes: Bytes, emit: (Frame) -> Void) where Bytes.Element == UInt8 {
        var input = ArraySlice(Array(bytes))
        while !input.isEmpty {
            switch phase {
            case .failed:
                return
            case .header:
                guard consumeHeader(&input) else { return }
            case .payload:
                consumePayload(&input, emit: emit)
            }
        }
    }

    /// Returns false when more bytes are needed for a complete frame header (decoder waits for the next read).
    private func consumeHeader(_ input: inout ArraySlice<UInt8>) -> Bool {
        while header.count < requiredHeaderBytes(header) {
            guard let byte = input.first else { return false }
            header.append(byte)
            input = input.dropFirst()
        }
        return finalizeHeader()
    }

    /// The full header length is discovered progressively: 2 base bytes, +2/+8 for extended length, +4 if masked.
    private func requiredHeaderBytes(_ current: [UInt8]) -> Int {
        guard current.count >= 2 else { return 2 }
        let masked = current[1] & 0x80 != 0
        let len = current[1] & 0x7F
        var needed = 2
        if len == 126 { needed += 2 } else if len == 127 { needed += 8 }
        if masked { needed += 4 }
        return needed
    }

    private func finalizeHeader() -> Bool {
        let byte0 = header[0]
        fin = byte0 & 0x80 != 0
        compressed = byte0 & 0x40 != 0
        guard let code = WebSocketOpcode(rawValue: byte0 & 0x0F) else {
            phase = .failed
            return false
        }
        opcode = code
        masked = header[1] & 0x80 != 0
        let len = header[1] & 0x7F
        var cursor = 2
        var length = 0
        if len == 126 {
            length = Int(header[2]) << 8 | Int(header[3])
            cursor = 4
        } else if len == 127 {
            for offset in 0..<8 { length = length << 8 | Int(header[2 + offset]) }
            cursor = 10
        } else {
            length = Int(len)
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
        header = []
        phase = .payload
        return true
    }

    private func consumePayload(_ input: inout ArraySlice<UInt8>, emit: (Frame) -> Void) {
        let take = min(remaining, input.count)
        if take > 0 {
            let chunk = input.prefix(take)
            for byte in chunk {
                let unmasked = masked ? byte ^ maskKey[maskIndex & 3] : byte
                maskIndex += 1
                if captured.count < captureLimit { captured.append(unmasked) }
            }
            input = input.dropFirst(take)
            remaining -= take
        }
        guard remaining == 0 else { return }
        emit(makeFrame())
        phase = .header
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

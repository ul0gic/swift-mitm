struct WebSocketExchange: Sendable {
    let originRequestHead: [UInt8]
    let clientResponseHead: [UInt8]
    let originReceivedFrames: [UInt8]
    let clientReceivedFrames: [UInt8]
    let peerCertificateDER: [UInt8]
}

enum WebSocketWire {
    static let clientTextPayload = Array("client text".utf8)
    static let serverBinaryPayload: [UInt8] = [0xDE, 0xAD, 0xBE, 0xEF]
    static let closePayload = [UInt8(0x03), 0xE8] + Array("done".utf8)
    static let clientTextFrame = maskedFrame(opcode: 0x1, payload: clientTextPayload, key: [1, 2, 3, 4])
    static let clientCloseFrame = maskedFrame(opcode: 0x8, payload: closePayload, key: [5, 6, 7, 8])
    static let serverBinaryFrame = unmaskedFrame(opcode: 0x2, payload: serverBinaryPayload)
    static let serverCloseFrame = unmaskedFrame(opcode: 0x8, payload: closePayload)
    static let clientFrames = clientTextFrame + clientCloseFrame
    static let serverFrames = serverBinaryFrame + serverCloseFrame
    static let responseHead = Array(
        (
            "HTTP/1.1 101 Switching Protocols\r\n"
            + "Upgrade: websocket\r\n"
            + "Connection: Upgrade\r\n"
            + "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
        ).utf8
    )

    static func requestHead(originHost: String, originPort: Int) -> [UInt8] {
        Array(
            (
                "GET /socket HTTP/1.1\r\n"
                + "Host: \(originHost):\(originPort)\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
            ).utf8
        )
    }

    private static func maskedFrame(opcode: UInt8, payload: [UInt8], key: [UInt8]) -> [UInt8] {
        [0x80 | opcode, 0x80 | UInt8(payload.count)]
            + key
            + payload.enumerated().map { $0.element ^ key[$0.offset & 3] }
    }

    private static func unmaskedFrame(opcode: UInt8, payload: [UInt8]) -> [UInt8] {
        [0x80 | opcode, UInt8(payload.count)] + payload
    }
}

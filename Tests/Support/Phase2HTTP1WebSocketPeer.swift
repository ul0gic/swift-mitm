enum Phase2HTTP1WebSocketPeer {
    static let requestBytes = Array([
        "GET /socket HTTP/1.1\r\n",
        "Host: localhost\r\n",
        "Connection: Upgrade\r\n",
        "Upgrade: websocket\r\n",
        "Sec-WebSocket-Version: 13\r\n",
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    ]
    .joined()
    .utf8)
    static let responseBytes = Array([
        "HTTP/1.1 101 Switching Protocols\r\n",
        "Connection: Upgrade\r\n",
        "Upgrade: websocket\r\n",
        "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
    ]
    .joined()
    .utf8)
}

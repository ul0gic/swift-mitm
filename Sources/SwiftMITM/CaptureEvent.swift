import Foundation

public enum HTTPProtocolVersion: String, Sendable {
    case http11 = "HTTP/1.1"
    case http2 = "HTTP/2"
}

public struct HTTPHeaderField: Sendable, Hashable {
    public let name: String
    public let value: String

    public init(name: String, value: String) {
        self.name = name
        self.value = value
    }
}

public struct CapturedRequestHead: Sendable {
    public let id: UUID
    public let timestamp: Date
    public let scheme: String
    public let authority: String
    public let method: String
    public let path: String
    public let version: HTTPProtocolVersion
    public let headers: [HTTPHeaderField]

    public init(
        id: UUID,
        timestamp: Date,
        scheme: String,
        authority: String,
        method: String,
        path: String,
        version: HTTPProtocolVersion,
        headers: [HTTPHeaderField]
    ) {
        self.id = id
        self.timestamp = timestamp
        self.scheme = scheme
        self.authority = authority
        self.method = method
        self.path = path
        self.version = version
        self.headers = headers
    }
}

public struct CapturedResponseHead: Sendable {
    public let requestID: UUID
    public let timestamp: Date
    public let status: Int
    public let version: HTTPProtocolVersion
    public let headers: [HTTPHeaderField]

    public init(
        requestID: UUID,
        timestamp: Date,
        status: Int,
        version: HTTPProtocolVersion,
        headers: [HTTPHeaderField]
    ) {
        self.requestID = requestID
        self.timestamp = timestamp
        self.status = status
        self.version = version
        self.headers = headers
    }
}

public enum WebSocketDirection: String, Sendable {
    case clientToServer
    case serverToClient
}

public enum WebSocketOpcode: UInt8, Sendable {
    case continuation = 0x0
    case text = 0x1
    case binary = 0x2
    case connectionClose = 0x8
    case ping = 0x9
    case pong = 0xA

    public init?(rawValue: UInt8) {
        switch rawValue {
        case 0x0: self = .continuation
        case 0x1: self = .text
        case 0x2: self = .binary
        case 0x8: self = .connectionClose
        case 0x9: self = .ping
        case 0xA: self = .pong
        default: return nil
        }
    }
}

/// One decoded RFC 6455 frame. `bytes` is the payload as it appears on the wire (unmasked); when the connection
/// negotiated permessage-deflate and `compressed` is true, those bytes are still DEFLATE-compressed (not inflated).
public struct CapturedWebSocketFrame: Sendable {
    public let connectionID: UUID
    public let timestamp: Date
    public let direction: WebSocketDirection
    public let opcode: WebSocketOpcode
    public let fin: Bool
    public let compressed: Bool
    public let bytes: [UInt8]
    public let byteCount: Int
    public let truncated: Bool
    public let closeCode: Int?
    public let closeReason: String?

    public init(
        connectionID: UUID,
        timestamp: Date,
        direction: WebSocketDirection,
        opcode: WebSocketOpcode,
        fin: Bool,
        compressed: Bool,
        bytes: [UInt8],
        byteCount: Int,
        truncated: Bool,
        closeCode: Int? = nil,
        closeReason: String? = nil
    ) {
        self.connectionID = connectionID
        self.timestamp = timestamp
        self.direction = direction
        self.opcode = opcode
        self.fin = fin
        self.compressed = compressed
        self.bytes = bytes
        self.byteCount = byteCount
        self.truncated = truncated
        self.closeCode = closeCode
        self.closeReason = closeReason
    }
}

public enum CaptureEvent: Sendable {
    case requestHead(CapturedRequestHead)
    /// `bytes` is the captured (bounded) slice of this chunk; `byteCount` is the chunk's full size.
    case requestBodyChunk(requestID: UUID, bytes: [UInt8], byteCount: Int)
    /// `truncated` is true when the full body exceeded the capture limit, so `bytes` are partial.
    case requestEnd(requestID: UUID, truncated: Bool)
    case responseHead(CapturedResponseHead)
    /// `bytes` is the captured (bounded) slice of this chunk; `byteCount` is the chunk's full size.
    case responseBodyChunk(requestID: UUID, bytes: [UInt8], byteCount: Int)
    /// `truncated` is true when the full body exceeded the capture limit, so `bytes` are partial.
    case responseEnd(requestID: UUID, truncated: Bool)
    case streamError(requestID: UUID, message: String)
    /// The `Upgrade: websocket` + `101` handshake completed; `requestID` is the shared connection id for its frames.
    case webSocketOpen(connectionID: UUID, timestamp: Date, permessageDeflate: Bool)
    case webSocketFrame(CapturedWebSocketFrame)
    case webSocketClose(connectionID: UUID, timestamp: Date, code: Int?, reason: String?)
}

public protocol CaptureEventSink: Sendable {
    func receive(_ event: CaptureEvent)
}

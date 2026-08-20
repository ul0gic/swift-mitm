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

public struct CapturedNetworkEndpoint: Sendable {
    public let address: String
    public let port: Int

    public init(address: String, port: Int) {
        self.address = address
        self.port = port
    }
}

public enum CapturedIngressProvenance: String, Sendable {
    case explicitConnect
    case trustedProxyV2
}

public struct CapturedTarget: Sendable {
    public let destination: CapturedNetworkEndpoint
    public let logicalAuthority: String
    public let tlsServerName: String?
    public let ingressProvenance: CapturedIngressProvenance
    public let originalClient: CapturedNetworkEndpoint?

    public init(
        destination: CapturedNetworkEndpoint,
        logicalAuthority: String,
        tlsServerName: String? = nil,
        ingressProvenance: CapturedIngressProvenance,
        originalClient: CapturedNetworkEndpoint? = nil
    ) {
        self.destination = destination
        self.logicalAuthority = logicalAuthority
        self.tlsServerName = tlsServerName
        self.ingressProvenance = ingressProvenance
        self.originalClient = originalClient
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
    public let target: CapturedTarget?

    public init(
        id: UUID,
        timestamp: Date,
        scheme: String,
        authority: String,
        method: String,
        path: String,
        version: HTTPProtocolVersion,
        headers: [HTTPHeaderField],
        target: CapturedTarget? = nil
    ) {
        self.id = id
        self.timestamp = timestamp
        self.scheme = scheme
        self.authority = authority
        self.method = method
        self.path = path
        self.version = version
        self.headers = headers
        self.target = target
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

public struct CapturedOpaqueFlow: Sendable {
    public let id: UUID
    public let timestamp: Date
    public let target: CapturedTarget

    public init(id: UUID, timestamp: Date, target: CapturedTarget) {
        self.id = id
        self.timestamp = timestamp
        self.target = target
    }
}

public enum OpaqueFlowDirection: String, Sendable {
    case clientToServer
    case serverToClient
}

public enum OpaqueFlowCloseReason: String, Sendable {
    case completed
    case cancelled
}

public enum CapturedConnectionFailureReason: String, Sendable {
    case untrustedPeer
    case malformedProxyMetadata
    case unsupportedProxyTransport
    case classificationFailed
    case destinationUnavailable
    case upstreamConnectionFailed
    case tlsHandshakeFailed
    case transportFailure
    case timedOut
    case cancelled
}

public struct CapturedConnectionFailure: Sendable {
    public let id: UUID
    public let timestamp: Date
    public let reason: CapturedConnectionFailureReason
    public let target: CapturedTarget?

    public init(
        id: UUID,
        timestamp: Date,
        reason: CapturedConnectionFailureReason,
        target: CapturedTarget? = nil
    ) {
        self.id = id
        self.timestamp = timestamp
        self.reason = reason
        self.target = target
    }
}

public enum CaptureEvent: Sendable {
    case requestHead(CapturedRequestHead)
    case requestBodyChunk(requestID: UUID, bytes: [UInt8], byteCount: Int)
    case requestTrailers(requestID: UUID, headers: [HTTPHeaderField])
    case requestEnd(requestID: UUID, truncated: Bool)
    case responseHead(CapturedResponseHead)
    case responseBodyChunk(requestID: UUID, bytes: [UInt8], byteCount: Int)
    case responseTrailers(requestID: UUID, headers: [HTTPHeaderField])
    case responseEnd(requestID: UUID, truncated: Bool)
    case streamError(requestID: UUID, message: String)
    case webSocketOpen(connectionID: UUID, timestamp: Date, permessageDeflate: Bool)
    case webSocketFrame(CapturedWebSocketFrame)
    case webSocketClose(connectionID: UUID, timestamp: Date, code: Int?, reason: String?)
    case opaqueOpen(CapturedOpaqueFlow)
    // swiftlint:disable:next enum_case_associated_values_count
    case opaqueData(
        flowID: UUID,
        timestamp: Date,
        direction: OpaqueFlowDirection,
        bytes: [UInt8],
        byteCount: Int
    ) // Frozen public event shape preserves direct chunk construction.
    // swiftlint:disable:next enum_case_associated_values_count
    case opaqueDirectionEnd(
        flowID: UUID,
        timestamp: Date,
        direction: OpaqueFlowDirection,
        byteCount: Int,
        truncated: Bool
    ) // Frozen public event shape preserves direct terminal construction.
    case opaqueClose(flowID: UUID, timestamp: Date, reason: OpaqueFlowCloseReason)
    case opaqueError(flowID: UUID, timestamp: Date, reason: CapturedConnectionFailureReason)
    case connectionFailure(CapturedConnectionFailure)
}

public protocol CaptureEventSink: Sendable {
    func receive(_ event: CaptureEvent)
}

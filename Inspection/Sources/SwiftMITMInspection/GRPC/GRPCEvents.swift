public enum GRPCWireFormat: Sendable { case native, webBinary, webText }

public struct GRPCMessage: Sendable {
    public let ordinal: UInt64
    public let envelopeOffset: UInt64
    public let encodedRange: Range<UInt64>
    public let compressed: Bool
    public let bytes: [UInt8]
}

public struct GRPCStatus: Sendable {
    public let httpStatus: Int?
    public let code: Int?
    public let rawMessage: String?
    public let message: String?
    public let details: String?
    public let metadata: [(String, String)]
}

public enum GRPCDiagnostic: Error, Sendable {
    case malformedEnvelope, limitExceeded, invalidBase64, incompleteTransport
    case unsupportedCompression, invalidTrailers, missingStatus, invalidStatus, duplicateStatus
}

public enum GRPCStreamEvent: Sendable {
    case message(GRPCMessage), status(GRPCStatus), diagnostic(GRPCDiagnostic)
}

public struct GRPCDecoderProgress: Sendable {
    public let consumedBytes: Int
    public let producedBytes: Int
    public let outputLimit: Bool
}

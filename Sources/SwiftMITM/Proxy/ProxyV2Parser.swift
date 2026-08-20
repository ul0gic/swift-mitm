import NIOCore

struct ProxyV2Metadata: Equatable, Sendable {
    let sourceAddress: SocketAddress
    let destinationAddress: SocketAddress
    let tlvCount: Int
}

enum ProxyV2ParseResult: Equatable, Sendable {
    case pending
    case complete(ProxyV2Metadata)
}

enum ProxyV2ParserError: Error, Equatable, Sendable {
    case invalidSignature
    case unsupportedVersion(UInt8)
    case unsupportedCommand(UInt8)
    case unsupportedAddressFamily(UInt8)
    case unsupportedTransport(UInt8)
    case headerTooLarge(declared: Int, maximum: Int)
    case malformedAddressBlock
    case invalidPort
    case malformedTLV
    case truncatedHeader
    case parserAlreadyCompleted
}

struct ProxyV2Parser {
    static let defaultMaximumHeaderBytes = 4_096

    private static let signature: [UInt8] = [
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A
    ]
    private static let fixedHeaderBytes = 16
    private static let maximumProtocolHeaderBytes = fixedHeaderBytes + Int(UInt16.max)

    private let maximumHeaderBytes: Int
    private var header: ByteBuffer
    private var expectedHeaderBytes: Int?
    private var completed = false

    init(maximumHeaderBytes: Int = defaultMaximumHeaderBytes) {
        precondition(
            maximumHeaderBytes >= Self.fixedHeaderBytes
                && maximumHeaderBytes <= Self.maximumProtocolHeaderBytes,
            "PROXY v2 header limit is outside the protocol range"
        )
        self.maximumHeaderBytes = maximumHeaderBytes
        header = ByteBufferAllocator().buffer(capacity: min(maximumHeaderBytes, Self.fixedHeaderBytes))
    }

    mutating func parse(_ bytes: inout ByteBuffer) throws -> ProxyV2ParseResult {
        guard !completed else { throw ProxyV2ParserError.parserAlreadyCompleted }

        copyRequiredBytes(from: &bytes, targetByteCount: Self.fixedHeaderBytes)
        try validateSignaturePrefix()
        guard header.readableBytes >= Self.fixedHeaderBytes else { return .pending }

        if expectedHeaderBytes == nil {
            expectedHeaderBytes = try parseExpectedHeaderBytes()
        }
        guard let expectedHeaderBytes else { return .pending }
        copyRequiredBytes(from: &bytes, targetByteCount: expectedHeaderBytes)
        guard header.readableBytes == expectedHeaderBytes else { return .pending }

        let metadata = try parseCompleteHeader()
        completed = true
        return .complete(metadata)
    }

    func finish() throws {
        guard completed else { throw ProxyV2ParserError.truncatedHeader }
    }

    private mutating func copyRequiredBytes(from bytes: inout ByteBuffer, targetByteCount: Int) {
        let missingByteCount = targetByteCount - header.readableBytes
        guard missingByteCount > 0 else { return }
        let copyByteCount = min(missingByteCount, bytes.readableBytes)
        guard var slice = bytes.readSlice(length: copyByteCount) else { return }
        header.writeBuffer(&slice)
    }

    private func validateSignaturePrefix() throws {
        let comparedByteCount = min(header.readableBytes, Self.signature.count)
        guard
            let received = header.getBytes(at: header.readerIndex, length: comparedByteCount),
            received.elementsEqual(Self.signature.prefix(comparedByteCount))
        else {
            throw ProxyV2ParserError.invalidSignature
        }
    }

    private func parseExpectedHeaderBytes() throws -> Int {
        let baseIndex = header.readerIndex
        guard
            let versionAndCommand: UInt8 = header.getInteger(at: baseIndex + 12),
            let familyAndTransport: UInt8 = header.getInteger(at: baseIndex + 13),
            let declaredLength: UInt16 = header.getInteger(at: baseIndex + 14, endianness: .big)
        else {
            throw ProxyV2ParserError.truncatedHeader
        }

        let version = versionAndCommand >> 4
        guard version == 2 else { throw ProxyV2ParserError.unsupportedVersion(version) }
        let command = versionAndCommand & 0x0F
        guard command == 1 else { throw ProxyV2ParserError.unsupportedCommand(command) }

        let addressFamily = familyAndTransport >> 4
        guard addressFamily == 1 || addressFamily == 2 else {
            throw ProxyV2ParserError.unsupportedAddressFamily(addressFamily)
        }
        let transport = familyAndTransport & 0x0F
        guard transport == 1 else { throw ProxyV2ParserError.unsupportedTransport(transport) }

        let completeHeaderBytes = Self.fixedHeaderBytes + Int(declaredLength)
        guard completeHeaderBytes <= maximumHeaderBytes else {
            throw ProxyV2ParserError.headerTooLarge(declared: completeHeaderBytes, maximum: maximumHeaderBytes)
        }
        let addressByteCount = addressFamily == 1 ? 12 : 36
        guard Int(declaredLength) >= addressByteCount else {
            throw ProxyV2ParserError.malformedAddressBlock
        }
        return completeHeaderBytes
    }

    private func parseCompleteHeader() throws -> ProxyV2Metadata {
        let baseIndex = header.readerIndex
        guard let familyAndTransport: UInt8 = header.getInteger(at: baseIndex + 13) else {
            throw ProxyV2ParserError.truncatedHeader
        }
        let addressByteCount = familyAndTransport >> 4 == 1 ? 4 : 16
        let addressBlockIndex = baseIndex + Self.fixedHeaderBytes
        let sourceAddress = try parseAddress(
            at: addressBlockIndex,
            byteCount: addressByteCount,
            portIndex: addressBlockIndex + (addressByteCount * 2)
        )
        let destinationAddress = try parseAddress(
            at: addressBlockIndex + addressByteCount,
            byteCount: addressByteCount,
            portIndex: addressBlockIndex + (addressByteCount * 2) + 2
        )
        let tlvIndex = addressBlockIndex + (addressByteCount * 2) + 4
        let tlvCount = try validateTLVs(from: tlvIndex, through: baseIndex + header.readableBytes)
        return ProxyV2Metadata(
            sourceAddress: sourceAddress,
            destinationAddress: destinationAddress,
            tlvCount: tlvCount
        )
    }

    private func parseAddress(at index: Int, byteCount: Int, portIndex: Int) throws -> SocketAddress {
        guard
            let packedAddress = header.getSlice(at: index, length: byteCount),
            let port: UInt16 = header.getInteger(at: portIndex, endianness: .big)
        else {
            throw ProxyV2ParserError.malformedAddressBlock
        }
        guard port != 0 else { throw ProxyV2ParserError.invalidPort }
        do {
            return try SocketAddress(packedIPAddress: packedAddress, port: Int(port))
        } catch {
            throw ProxyV2ParserError.malformedAddressBlock
        }
    }

    private func validateTLVs(from startIndex: Int, through endIndex: Int) throws -> Int {
        var index = startIndex
        var count = 0
        while index < endIndex {
            guard endIndex - index >= 3 else { throw ProxyV2ParserError.malformedTLV }
            guard let length: UInt16 = header.getInteger(at: index + 1, endianness: .big) else {
                throw ProxyV2ParserError.malformedTLV
            }
            let nextIndex = index + 3 + Int(length)
            guard nextIndex <= endIndex else { throw ProxyV2ParserError.malformedTLV }
            count += 1
            index = nextIndex
        }
        return count
    }
}

struct ProxyV2ParserBoundary {
    static let deadline = TimeAmount.seconds(5)

    var parser = ProxyV2Parser()
}

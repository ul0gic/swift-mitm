// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2017-2022 Apple Inc. and the SwiftNIO project authors

import NIOCore
import NIOHTTP1

#if compiler(>=6.1)
private import CSwiftMITMLLHTTP
#else
@_implementationOnly import CSwiftMITMLLHTTP
#endif

fileprivate enum ParserKeepAliveState {
    case keepAlive
    case close
}

extension UnsafeMutablePointer where Pointee == swiftmitm_llhttp_t {

    fileprivate var keepAliveState: ParserKeepAliveState {
        c_swiftmitm_llhttp_should_keep_alive(self) == 0 ? .close : .keepAlive
    }
}

private enum HTTPDecodingState {
    case beforeMessageBegin
    case afterMessageBegin
    case url
    case headerName
    case headerValue
    case trailerName
    case trailerValue
    case headersComplete
}

private class BetterHTTPParser {
    private let maximumHeaderFieldSize: Int
    private let maximumTotalHeadersSize: Int
    private let maximumHeaderFieldCount: Int

    var delegate: HTTPDecoderDelegate! = nil
    private var parser: swiftmitm_llhttp_t? = swiftmitm_llhttp_t()
    private var settings: UnsafeMutablePointer<swiftmitm_llhttp_settings_t>
    private var decodingState: HTTPDecodingState = .beforeMessageBegin
    private var firstNonDiscardableOffset: Int? = nil
    private var currentFieldByteLength = 0
    private var totalHeadersByteLength = 0
    private var headerFieldCount = 0
    private var httpParserOffset = 0
    private var rawBytesView: UnsafeRawBufferPointer = .init(start: UnsafeRawPointer(bitPattern: 0xcafbabe), count: 0)
    private var httpErrno: swiftmitm_llhttp_errno_t? = nil
    private var richerError: Error? = nil
    private let kind: HTTPDecoderKind
    var requestHeads = CircularBuffer<HTTPRequestHead>(initialCapacity: 1)

    enum MessageContinuation {
        case normal
        case skipBody
        case error(swiftmitm_llhttp_errno_t)
    }

    private static func fromOpaque(_ opaque: UnsafePointer<swiftmitm_llhttp_t>?) -> BetterHTTPParser {
        Unmanaged<BetterHTTPParser>.fromOpaque(UnsafeRawPointer(opaque!.pointee.data)).takeUnretainedValue()
    }

    init(kind: HTTPDecoderKind, configuration: NIOHTTPDecoderLimitConfiguration) {
        self.kind = kind
        self.maximumHeaderFieldSize = configuration.maxHeaderFieldSize
        self.maximumTotalHeadersSize = configuration.maxHeaderListSize
        self.maximumHeaderFieldCount = configuration.maxHeaderFieldCount
        self.settings = UnsafeMutablePointer.allocate(capacity: 1)
        c_swiftmitm_llhttp_settings_init(self.settings)
        self.settings.pointee.on_body = { opaque, bytes, len in
            BetterHTTPParser.fromOpaque(opaque).didReceiveBodyData(UnsafeRawBufferPointer(start: bytes, count: len))
            return 0
        }
        self.settings.pointee.on_header_field = { opaque, bytes, len in
            BetterHTTPParser.fromOpaque(opaque).didReceiveHeaderFieldData(
                UnsafeRawBufferPointer(start: bytes, count: len)
            )
        }
        self.settings.pointee.on_header_value = { opaque, bytes, len in
            BetterHTTPParser.fromOpaque(opaque).didReceiveHeaderValueData(
                UnsafeRawBufferPointer(start: bytes, count: len)
            )
        }
        self.settings.pointee.on_status = { opaque, bytes, len in
            BetterHTTPParser.fromOpaque(opaque).didReceiveStatusData(UnsafeRawBufferPointer(start: bytes, count: len))
            return 0
        }
        self.settings.pointee.on_url = { opaque, bytes, len in
            BetterHTTPParser.fromOpaque(opaque).didReceiveURLData(UnsafeRawBufferPointer(start: bytes, count: len))
        }
        self.settings.pointee.on_chunk_complete = { opaque in
            BetterHTTPParser.fromOpaque(opaque).didReceiveChunkCompleteNotification()
            return 0
        }
        self.settings.pointee.on_chunk_header = { opaque in
            BetterHTTPParser.fromOpaque(opaque).didReceiveChunkHeaderNotification()
            return 0
        }
        self.settings.pointee.on_message_begin = { opaque in
            BetterHTTPParser.fromOpaque(opaque).didReceiveMessageBeginNotification()
            return 0
        }
        self.settings.pointee.on_headers_complete = { opaque in
            let parser = BetterHTTPParser.fromOpaque(opaque)
            switch parser.didReceiveHeadersCompleteNotification(
                versionMajor: Int(opaque!.pointee.http_major),
                versionMinor: Int(opaque!.pointee.http_minor),
                statusCode: Int(opaque!.pointee.status_code),
                isUpgrade: opaque!.pointee.upgrade != 0,
                method: swiftmitm_llhttp_method(rawValue: numericCast(opaque!.pointee.method)),
                keepAliveState: opaque!.keepAliveState
            ) {
            case .normal:
                return 0
            case .skipBody:
                return 1
            case .error(let err):
                parser.httpErrno = err
                return -1
            }
        }
        self.settings.pointee.on_message_complete = { opaque in
            BetterHTTPParser.fromOpaque(opaque).didReceiveMessageCompleteNotification()
            // llhttp requires clearing content_length here: nodejs/llhttp#202, apple/swift-nio#2274.
            opaque?.pointee.content_length = 0
            return 0
        }
        self.withExclusiveHTTPParser { parserPtr in
            switch kind {
            case .request:
                c_swiftmitm_llhttp_init(parserPtr, SWIFTMITM_HTTP_REQUEST, self.settings)
            case .response:
                c_swiftmitm_llhttp_init(parserPtr, SWIFTMITM_HTTP_RESPONSE, self.settings)
            }
        }
    }

    deinit {
        self.settings.deallocate()
    }

    private func start(bytes: UnsafeRawBufferPointer, newState: HTTPDecodingState) {
        assert(self.firstNonDiscardableOffset == nil)
        self.firstNonDiscardableOffset = bytes.baseAddress! - self.rawBytesView.baseAddress!
        self.decodingState = newState
    }

    private func finish(_ callout: (inout HTTPDecoderDelegate, UnsafeRawBufferPointer) throws -> Void) rethrows {
        var currentFieldByteLength = 0
        swap(&currentFieldByteLength, &self.currentFieldByteLength)
        let start = self.rawBytesView.startIndex + self.firstNonDiscardableOffset!
        let end = start + currentFieldByteLength
        self.firstNonDiscardableOffset = nil
        precondition(start >= self.rawBytesView.startIndex && end <= self.rawBytesView.endIndex)
        try callout(&self.delegate, .init(rebasing: self.rawBytesView[start..<end]))
    }

    private func didReceiveBodyData(_ bytes: UnsafeRawBufferPointer) {
        self.delegate.didReceiveBody(bytes)
    }

    private func didReceiveHeaderFieldData(_ bytes: UnsafeRawBufferPointer) -> CInt {
        var isNewField = false
        switch self.decodingState {
        case .headerName, .trailerName:
            ()
        case .headerValue:
            self.finish { delegate, bytes in
                delegate.didReceiveHeaderValue(bytes)
            }
            self.start(bytes: bytes, newState: .headerName)
            isNewField = true
        case .trailerValue:
            self.finish { delegate, bytes in
                delegate.didReceiveTrailerValue(bytes)
            }
            self.start(bytes: bytes, newState: .trailerName)
            isNewField = true
        case .url:
            self.finish { delegate, bytes in
                delegate.didReceiveURL(bytes)
            }
            self.start(bytes: bytes, newState: .headerName)
            isNewField = true
        case .headersComplete:

            self.start(bytes: bytes, newState: .trailerName)
            isNewField = true
        case .afterMessageBegin:

            self.start(bytes: bytes, newState: .headerName)
            isNewField = true
        case .beforeMessageBegin:
            preconditionFailure()
        }
        if isNewField {
            self.headerFieldCount += 1
            if self.headerFieldCount > self.maximumHeaderFieldCount {
                self.richerError = HTTPParserError.headerOverflow
                return -1
            }
        }
        return self.validateHeaderLength(bytes.count)
    }

    private func didReceiveHeaderValueData(_ bytes: UnsafeRawBufferPointer) -> CInt {
        do {
            switch self.decodingState {
            case .headerValue, .trailerValue:
                ()
            case .headerName:
                try self.finish { delegate, bytes in
                    try delegate.didReceiveHeaderName(bytes)
                }
                self.start(bytes: bytes, newState: .headerValue)
            case .trailerName:
                try self.finish { delegate, bytes in
                    try delegate.didReceiveTrailerName(bytes)
                }
                self.start(bytes: bytes, newState: .trailerValue)
            case .beforeMessageBegin, .afterMessageBegin, .headersComplete, .url:
                preconditionFailure()
            }
            return self.validateHeaderLength(bytes.count)
        } catch {
            self.richerError = error
            return -1
        }
    }

    private func didReceiveStatusData(_ bytes: UnsafeRawBufferPointer) {

    }

    private func didReceiveURLData(_ bytes: UnsafeRawBufferPointer) -> CInt {
        switch self.decodingState {
        case .url:
            ()
        case .afterMessageBegin:
            self.start(bytes: bytes, newState: .url)
        case .beforeMessageBegin, .headersComplete, .headerName, .headerValue, .trailerName, .trailerValue:
            preconditionFailure()
        }
        return self.validateHeaderLength(bytes.count)
    }

    private func didReceiveChunkCompleteNotification() {

    }

    private func didReceiveChunkHeaderNotification() {

    }

    private func didReceiveMessageBeginNotification() {
        switch self.decodingState {
        case .beforeMessageBegin:
            self.totalHeadersByteLength = 0
            self.headerFieldCount = 0
            self.decodingState = .afterMessageBegin
        case .headersComplete, .headerName, .headerValue, .trailerName, .trailerValue, .afterMessageBegin, .url:
            preconditionFailure()
        }
    }

    private func didReceiveMessageCompleteNotification() {
        switch self.decodingState {
        case .headersComplete:
            ()
        case .trailerValue:
            self.finish { delegate, bytes in
                delegate.didReceiveTrailerValue(bytes)
            }
        case .beforeMessageBegin, .headerName, .headerValue, .trailerName, .afterMessageBegin, .url:
            preconditionFailure()
        }
        self.decodingState = .beforeMessageBegin
        self.delegate.didFinishMessage()
    }

    private func didReceiveHeadersCompleteNotification(
        versionMajor: Int,
        versionMinor: Int,
        statusCode: Int,
        isUpgrade: Bool,
        method: swiftmitm_llhttp_method,
        keepAliveState: ParserKeepAliveState
    ) -> MessageContinuation {
        switch self.decodingState {
        case .headerValue:
            self.finish { delegate, bytes in
                delegate.didReceiveHeaderValue(bytes)
            }
        case .url:
            self.finish { delegate, bytes in
                delegate.didReceiveURL(bytes)
            }
        case .afterMessageBegin:

            precondition(self.kind == .response)
        case .beforeMessageBegin, .headersComplete, .headerName, .trailerName, .trailerValue:
            preconditionFailure()
        }
        assert(self.firstNonDiscardableOffset == nil)
        self.decodingState = .headersComplete

        var skipBody = false

        if self.kind == .response {

            guard !self.requestHeads.isEmpty else {
                self.richerError = NIOHTTPDecoderError.unsolicitedResponse
                return .error(SWIFTMITM_HPE_INTERNAL)
            }

            if 100 <= statusCode && statusCode < 200 && statusCode != 101 {

                skipBody = true
            } else {
                let method = self.requestHeads.removeFirst().method
                if method == .HEAD || method == .CONNECT {
                    skipBody = true
                } else if statusCode / 100 == 1
                    || statusCode == 204 || statusCode == 304
                {
                    skipBody = true
                }
            }
        }

        let success = self.delegate.didFinishHead(
            versionMajor: versionMajor,
            versionMinor: versionMinor,
            isUpgrade: isUpgrade,
            method: method,
            statusCode: statusCode,
            keepAliveState: keepAliveState
        )
        guard success else {
            return .error(SWIFTMITM_HPE_INVALID_VERSION)
        }

        return skipBody ? .skipBody : .normal
    }

    func start() {
        self.withExclusiveHTTPParser { parserPtr in
            parserPtr.pointee.data = Unmanaged.passRetained(self).toOpaque()
        }
    }

    func stop() {
        self.withExclusiveHTTPParser { parserPtr in
            let selfRef = parserPtr.pointee.data
            Unmanaged<BetterHTTPParser>.fromOpaque(selfRef!).release()
            parserPtr.pointee.data = UnsafeMutableRawPointer(bitPattern: 0xdedbeef)
        }
    }

    private func validateHeaderLength(_ newLength: Int) -> CInt {
        self.currentFieldByteLength += newLength
        if self.currentFieldByteLength > self.maximumHeaderFieldSize {
            self.richerError = HTTPParserError.headerOverflow
            return -1
        }

        self.totalHeadersByteLength += newLength
        if self.totalHeadersByteLength > self.maximumTotalHeadersSize {
            self.richerError = HTTPParserError.headerOverflow
            return -1
        }

        return 0
    }

    @inline(__always)
    func withExclusiveHTTPParser<T>(_ body: (UnsafeMutablePointer<swiftmitm_llhttp_t>) -> T) -> T {
        var parser: swiftmitm_llhttp_t? = nil
        assert(self.parser != nil, "parser must not be nil here, must be a re-entrancy issue")
        swap(&parser, &self.parser)
        defer {
            assert(self.parser == nil, "parser must not nil here")
            swap(&parser, &self.parser)
        }
        return body(&parser!)
    }

    func feedInput(_ bytes: UnsafeRawBufferPointer?) throws -> Int {
        var bytesRead = 0
        let parserErrno: swiftmitm_llhttp_errno_t = self.withExclusiveHTTPParser { parserPtr -> swiftmitm_llhttp_errno_t in
            var rc: swiftmitm_llhttp_errno_t

            if let bytes = bytes {
                self.rawBytesView = bytes
                defer {
                    self.rawBytesView = .init(start: UnsafeRawPointer(bitPattern: 0xdafbabe), count: 0)
                }

                let startPointer = bytes.baseAddress! + self.httpParserOffset
                let bytesToRead = bytes.count - self.httpParserOffset

                rc = c_swiftmitm_llhttp_execute_swift(
                    parserPtr,
                    startPointer,
                    bytesToRead
                )

                if rc == SWIFTMITM_HPE_PAUSED_UPGRADE {

                    bytesRead = UnsafeRawPointer(c_swiftmitm_llhttp_get_error_pos(parserPtr)!) - startPointer
                    c_swiftmitm_llhttp_resume_after_upgrade(parserPtr)
                    rc = SWIFTMITM_HPE_OK
                } else {
                    bytesRead = bytesToRead
                }
            } else {
                rc = c_swiftmitm_llhttp_finish(parserPtr)
                bytesRead = 0
            }

            return rc
        }

        guard parserErrno == SWIFTMITM_HPE_OK else {

            let err = self.httpErrno ?? parserErrno
            if err == SWIFTMITM_HPE_INTERNAL || err == SWIFTMITM_HPE_USER, let richerError = self.richerError {
                throw richerError
            } else {
                throw HTTPParserError.httpError(fromCHTTPParserErrno: err)!
            }
        }

        if let firstNonDiscardableOffset = self.firstNonDiscardableOffset {
            self.httpParserOffset += bytesRead - firstNonDiscardableOffset
            self.firstNonDiscardableOffset = 0
            return firstNonDiscardableOffset
        } else {

            let consumedBytes = self.httpParserOffset + bytesRead
            self.httpParserOffset = 0
            return consumedBytes
        }
    }
}

private protocol HTTPDecoderDelegate {
    mutating func didReceiveBody(_ bytes: UnsafeRawBufferPointer)
    mutating func didReceiveHeaderName(_ bytes: UnsafeRawBufferPointer) throws
    mutating func didReceiveHeaderValue(_ bytes: UnsafeRawBufferPointer)
    mutating func didReceiveTrailerName(_ bytes: UnsafeRawBufferPointer) throws
    mutating func didReceiveTrailerValue(_ bytes: UnsafeRawBufferPointer)
    mutating func didReceiveURL(_ bytes: UnsafeRawBufferPointer)
    mutating func didFinishHead(
        versionMajor: Int,
        versionMinor: Int,
        isUpgrade: Bool,
        method: swiftmitm_llhttp_method,
        statusCode: Int,
        keepAliveState: ParserKeepAliveState
    ) -> Bool
    mutating func didFinishMessage()
}

public typealias BytePreservingHTTPResponseDecoder = BytePreservingHTTPDecoder<HTTPClientResponsePart, HTTPClientRequestPart>

public typealias BytePreservingHTTPRequestDecoder = BytePreservingHTTPDecoder<HTTPServerRequestPart, HTTPServerResponsePart>

extension BytePreservingHTTPDecoder: WriteObservingByteToMessageDecoder
where In == HTTPClientResponsePart, Out == HTTPClientRequestPart {
    public typealias OutboundIn = Out

    public func write(data: HTTPClientRequestPart) {
        if case .head(let head) = data {
            self.parser.requestHeads.append(head)
        }
    }
}

public final class BytePreservingHTTPDecoder<In, Out>: ByteToMessageDecoder, HTTPDecoderDelegate {
    public typealias InboundOut = In

    private var headers: [(String, String)] = []
    private var trailers: [(String, String)]? = nil
    private var currentHeaderName: String? = nil
    private var url: String? = nil
    private var isUpgrade: Bool? = nil

    private var buffer: ByteBuffer? = nil
    private var context: ChannelHandlerContext? = nil

    private let parser: BetterHTTPParser
    private let leftOverBytesStrategy: RemoveAfterUpgradeStrategy
    private let informationalResponseStrategy: NIOInformationalResponseStrategy
    private let kind: HTTPDecoderKind
    private var stopParsing = false
    private var lastResponseHeaderWasInformational = false

    private var responseEncoder: BytePreservingHTTPResponseEncoder? = nil

    public init(
        leftOverBytesStrategy: RemoveAfterUpgradeStrategy = .dropBytes,
        informationalResponseStrategy: NIOInformationalResponseStrategy = .drop,
        limitConfiguration: NIOHTTPDecoderLimitConfiguration = .init(),
        responseEncoder: BytePreservingHTTPResponseEncoder? = nil
    ) {
        self.headers.reserveCapacity(16)
        if In.self == HTTPServerRequestPart.self {
            self.kind = .request
        } else if In.self == HTTPClientResponsePart.self {
            self.kind = .response
        } else {
            preconditionFailure("unknown HTTP message type \(In.self)")
        }
        self.parser = BetterHTTPParser(kind: kind, configuration: limitConfiguration)
        self.leftOverBytesStrategy = leftOverBytesStrategy
        self.informationalResponseStrategy = informationalResponseStrategy
        self.responseEncoder = responseEncoder
    }

    func didReceiveBody(_ bytes: UnsafeRawBufferPointer) {
        let offset = self.buffer!.withUnsafeReadableBytes { allBytes -> Int in
            let offset = bytes.baseAddress! - allBytes.baseAddress!
            assert(offset >= 0)
            assert(offset + bytes.count <= allBytes.count)
            return offset
        }
        self.buffer!.moveReaderIndex(forwardBy: offset)
        switch self.kind {
        case .request:
            self.context!.fireChannelRead(
                NIOAny(HTTPServerRequestPart.body(self.buffer!.readSlice(length: bytes.count)!))
            )
        case .response:
            self.context!.fireChannelRead(
                NIOAny(HTTPClientResponsePart.body(self.buffer!.readSlice(length: bytes.count)!))
            )
        }

    }

    func didReceiveHeaderName(_ bytes: UnsafeRawBufferPointer) throws {
        assert(self.currentHeaderName == nil)

        guard bytes.count > 0 else {
            throw HTTPParserError.invalidHeaderToken
        }
        self.currentHeaderName = String(decoding: bytes, as: Unicode.UTF8.self)
    }

    func didReceiveHeaderValue(_ bytes: UnsafeRawBufferPointer) {
        self.headers.append((self.currentHeaderName!, self.decodeHeaderFieldValue(bytes)))
        self.currentHeaderName = nil
    }

    func didReceiveTrailerName(_ bytes: UnsafeRawBufferPointer) throws {
        assert(self.currentHeaderName == nil)

        guard bytes.count > 0 else {
            throw HTTPParserError.invalidHeaderToken
        }
        self.currentHeaderName = String(decoding: bytes, as: Unicode.UTF8.self)
    }

    func didReceiveTrailerValue(_ bytes: UnsafeRawBufferPointer) {
        if self.trailers == nil {
            self.trailers = []
        }
        self.trailers?.append((self.currentHeaderName!, self.decodeHeaderFieldValue(bytes)))
        self.currentHeaderName = nil
    }

    private func decodeHeaderFieldValue(_ bytes: UnsafeRawBufferPointer) -> String {
        String(String.UnicodeScalarView(bytes.map { UnicodeScalar($0) }))
    }

    func didReceiveURL(_ bytes: UnsafeRawBufferPointer) {
        assert(self.url == nil)
        self.url = String(decoding: bytes, as: Unicode.UTF8.self)
    }

    fileprivate func didFinishHead(
        versionMajor: Int,
        versionMinor: Int,
        isUpgrade: Bool,
        method: swiftmitm_llhttp_method,
        statusCode: Int,
        keepAliveState: ParserKeepAliveState
    ) -> Bool {
        let message: NIOAny?

        guard versionMajor == 1 else {
            self.stopParsing = true
            self.context!.fireErrorCaught(HTTPParserError.invalidVersion)
            return false
        }

        switch self.kind {
        case .request:
            let reqHead = HTTPRequestHead(
                version: .init(major: versionMajor, minor: versionMinor),
                method: HTTPMethod.from(httpParserMethod: method),
                uri: self.url!,
                headers: HTTPHeaders(self.headers)
            )

            self.responseEncoder?.recordRequestMethod(reqHead.method)
            message = NIOAny(HTTPServerRequestPart.head(reqHead))

        case .response where (100..<200).contains(statusCode) && statusCode != 101:
            self.lastResponseHeaderWasInformational = true
            if self.informationalResponseStrategy == .forward {
                let resHeadPart = HTTPClientResponsePart.head(
                    versionMajor: versionMajor,
                    versionMinor: versionMinor,
                    statusCode: statusCode,
                    headers: self.headers
                )
                message = NIOAny(resHeadPart)
            } else {
                message = nil
            }

        case .response:
            self.lastResponseHeaderWasInformational = false
            let resHeadPart = HTTPClientResponsePart.head(
                versionMajor: versionMajor,
                versionMinor: versionMinor,
                statusCode: statusCode,
                headers: self.headers
            )
            message = NIOAny(resHeadPart)
        }
        self.url = nil
        self.headers.removeAll(keepingCapacity: true)
        if let message = message {
            self.context!.fireUserInboundEventTriggered(
                BytePreservingHTTP1MessageMetadata(isKeepAlive: keepAliveState == .keepAlive)
            )
            self.context!.fireChannelRead(message)
        }
        self.isUpgrade = isUpgrade
        return true
    }

    func didFinishMessage() {
        var trailers: [(String, String)]? = nil
        swap(&trailers, &self.trailers)
        switch self.kind {
        case .request:
            self.context!.fireChannelRead(NIOAny(HTTPServerRequestPart.end(trailers.map(HTTPHeaders.init))))
        case .response:
            if !self.lastResponseHeaderWasInformational {
                self.context!.fireChannelRead(NIOAny(HTTPClientResponsePart.end(trailers.map(HTTPHeaders.init))))
            }
        }
        self.stopParsing = self.isUpgrade!
        self.isUpgrade = nil
    }

    public func decoderAdded(context: ChannelHandlerContext) {
        self.parser.delegate = self
        self.parser.start()
    }

    public func decoderRemoved(context: ChannelHandlerContext) {
        self.parser.stop()
        self.parser.delegate = nil
    }

    private func feedEOF(context: ChannelHandlerContext) throws {
        self.context = context
        defer {
            self.context = nil
        }

        _ = try self.parser.feedInput(nil)
    }

    private func feedInput(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws {
        self.buffer = buffer
        self.context = context
        defer {
            self.buffer = nil
            self.context = nil
        }
        let consumed = try buffer.withUnsafeReadableBytes { bytes -> Int in
            try self.parser.feedInput(bytes)
        }
        buffer.moveReaderIndex(forwardBy: consumed)
    }

    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        if !self.stopParsing {
            try self.feedInput(context: context, buffer: &buffer)
        }
        return .needMoreData
    }

    public func decodeLast(
        context: ChannelHandlerContext,
        buffer: inout ByteBuffer,
        seenEOF: Bool
    ) throws -> DecodingState {
        if !self.stopParsing {
            while buffer.readableBytes > 0, case .continue = try self.decode(context: context, buffer: &buffer) {}
            if seenEOF {
                try self.feedEOF(context: context)
            }
        }
        if buffer.readableBytes > 0 && !seenEOF {

            switch self.leftOverBytesStrategy {
            case .dropBytes:
                ()
            case .fireError:
                context.fireErrorCaught(ByteToMessageDecoderError.leftoverDataWhenDone(buffer))
            case .forwardBytes:
                context.fireChannelRead(NIOAny(buffer))
            }
        }
        return .needMoreData
    }
}

@available(*, unavailable)
extension BytePreservingHTTPDecoder: Sendable {}

extension HTTPParserError {

    fileprivate static func httpError(fromCHTTPParserErrno: swiftmitm_llhttp_errno_t) -> HTTPParserError? {
        switch fromCHTTPParserErrno {
        case SWIFTMITM_HPE_INTERNAL:
            return .invalidInternalState
        case SWIFTMITM_HPE_STRICT:
            return .strictModeAssertion
        case SWIFTMITM_HPE_LF_EXPECTED:
            return .lfExpected
        case SWIFTMITM_HPE_UNEXPECTED_CONTENT_LENGTH:
            return .unexpectedContentLength
        case SWIFTMITM_HPE_CLOSED_CONNECTION:
            return .closedConnection
        case SWIFTMITM_HPE_INVALID_METHOD:
            return .invalidMethod
        case SWIFTMITM_HPE_INVALID_URL:
            return .invalidURL
        case SWIFTMITM_HPE_INVALID_CONSTANT:
            return .invalidConstant
        case SWIFTMITM_HPE_INVALID_VERSION:
            return .invalidVersion
        case SWIFTMITM_HPE_INVALID_HEADER_TOKEN,
            SWIFTMITM_HPE_UNEXPECTED_SPACE:
            return .invalidHeaderToken
        case SWIFTMITM_HPE_INVALID_CONTENT_LENGTH:
            return .invalidContentLength
        case SWIFTMITM_HPE_INVALID_CHUNK_SIZE:
            return .invalidChunkSize
        case SWIFTMITM_HPE_INVALID_STATUS:
            return .invalidStatus
        case SWIFTMITM_HPE_INVALID_EOF_STATE:
            return .invalidEOFState
        case SWIFTMITM_HPE_PAUSED, SWIFTMITM_HPE_PAUSED_UPGRADE, SWIFTMITM_HPE_PAUSED_H2_UPGRADE:
            return .paused
        case SWIFTMITM_HPE_INVALID_TRANSFER_ENCODING,
            SWIFTMITM_HPE_CR_EXPECTED,
            SWIFTMITM_HPE_CB_MESSAGE_BEGIN,
            SWIFTMITM_HPE_CB_HEADERS_COMPLETE,
            SWIFTMITM_HPE_CB_MESSAGE_COMPLETE,
            SWIFTMITM_HPE_CB_CHUNK_HEADER,
            SWIFTMITM_HPE_CB_CHUNK_COMPLETE,
            SWIFTMITM_HPE_USER,
            SWIFTMITM_HPE_CB_URL_COMPLETE,
            SWIFTMITM_HPE_CB_STATUS_COMPLETE,
            SWIFTMITM_HPE_CB_HEADER_FIELD_COMPLETE,
            SWIFTMITM_HPE_CB_HEADER_VALUE_COMPLETE:

            return .unknown
        default:
            return nil
        }
    }
}

extension HTTPMethod {

    fileprivate static func from(httpParserMethod: swiftmitm_llhttp_method) -> HTTPMethod {
        switch httpParserMethod {
        case SWIFTMITM_HTTP_DELETE:
            return .DELETE
        case SWIFTMITM_HTTP_GET:
            return .GET
        case SWIFTMITM_HTTP_HEAD:
            return .HEAD
        case SWIFTMITM_HTTP_POST:
            return .POST
        case SWIFTMITM_HTTP_PUT:
            return .PUT
        case SWIFTMITM_HTTP_CONNECT:
            return .CONNECT
        case SWIFTMITM_HTTP_OPTIONS:
            return .OPTIONS
        case SWIFTMITM_HTTP_TRACE:
            return .TRACE
        case SWIFTMITM_HTTP_COPY:
            return .COPY
        case SWIFTMITM_HTTP_LOCK:
            return .LOCK
        case SWIFTMITM_HTTP_MKCOL:
            return .MKCOL
        case SWIFTMITM_HTTP_MOVE:
            return .MOVE
        case SWIFTMITM_HTTP_PROPFIND:
            return .PROPFIND
        case SWIFTMITM_HTTP_PROPPATCH:
            return .PROPPATCH
        case SWIFTMITM_HTTP_SEARCH:
            return .SEARCH
        case SWIFTMITM_HTTP_UNLOCK:
            return .UNLOCK
        case SWIFTMITM_HTTP_BIND:
            return .BIND
        case SWIFTMITM_HTTP_REBIND:
            return .REBIND
        case SWIFTMITM_HTTP_UNBIND:
            return .UNBIND
        case SWIFTMITM_HTTP_ACL:
            return .ACL
        case SWIFTMITM_HTTP_REPORT:
            return .REPORT
        case SWIFTMITM_HTTP_MKACTIVITY:
            return .MKACTIVITY
        case SWIFTMITM_HTTP_CHECKOUT:
            return .CHECKOUT
        case SWIFTMITM_HTTP_MERGE:
            return .MERGE
        case SWIFTMITM_HTTP_MSEARCH:
            return .MSEARCH
        case SWIFTMITM_HTTP_NOTIFY:
            return .NOTIFY
        case SWIFTMITM_HTTP_SUBSCRIBE:
            return .SUBSCRIBE
        case SWIFTMITM_HTTP_UNSUBSCRIBE:
            return .UNSUBSCRIBE
        case SWIFTMITM_HTTP_PATCH:
            return .PATCH
        case SWIFTMITM_HTTP_PURGE:
            return .PURGE
        case SWIFTMITM_HTTP_MKCALENDAR:
            return .MKCALENDAR
        case SWIFTMITM_HTTP_LINK:
            return .LINK
        case SWIFTMITM_HTTP_UNLINK:
            return .UNLINK
        case SWIFTMITM_HTTP_SOURCE:

            return .RAW(value: "SOURCE")
        case SWIFTMITM_HTTP_PRI:
            return .RAW(value: "PRI")
        case SWIFTMITM_HTTP_DESCRIBE:
            return .RAW(value: "DESCRIBE")
        case SWIFTMITM_HTTP_ANNOUNCE:
            return .RAW(value: "ANNOUNCE")
        case SWIFTMITM_HTTP_SETUP:
            return .RAW(value: "SETUP")
        case SWIFTMITM_HTTP_PLAY:
            return .RAW(value: "PLAY")
        case SWIFTMITM_HTTP_PAUSE:
            return .RAW(value: "PAUSE")
        case SWIFTMITM_HTTP_TEARDOWN:
            return .RAW(value: "TEARDOWN")
        case SWIFTMITM_HTTP_GET_PARAMETER:
            return .RAW(value: "GET_PARAMETER")
        case SWIFTMITM_HTTP_SET_PARAMETER:
            return .RAW(value: "SET_PARAMETER")
        case SWIFTMITM_HTTP_REDIRECT:
            return .RAW(value: "REDIRECT")
        case SWIFTMITM_HTTP_RECORD:
            return .RAW(value: "RECORD")
        case SWIFTMITM_HTTP_FLUSH:
            return .RAW(value: "FLUSH")
        default:
            return .RAW(value: String(cString: c_swiftmitm_llhttp_method_name(httpParserMethod)))
        }
    }
}

extension HTTPClientResponsePart {
    fileprivate static func head(
        versionMajor: Int,
        versionMinor: Int,
        statusCode: Int,
        headers: [(String, String)]
    ) -> HTTPClientResponsePart {
        HTTPClientResponsePart.head(
            HTTPResponseHead(
                version: .init(major: versionMajor, minor: versionMinor),
                status: .init(statusCode: statusCode),
                headers: HTTPHeaders(headers)
            )
        )
    }
}

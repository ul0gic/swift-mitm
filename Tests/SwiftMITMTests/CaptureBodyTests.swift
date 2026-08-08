import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

@testable import SwiftMITM

private final class RecordingSink: CaptureEventSink, @unchecked Sendable {
    private(set) var events: [CaptureEvent] = []

    func receive(_ event: CaptureEvent) {
        events.append(event)
    }

    func body(for requestID: UUID, response: Bool) -> [UInt8] {
        events.reduce(into: [UInt8]()) { accumulator, event in
            switch event {
            case let .requestBodyChunk(id, bytes, _) where !response && id == requestID:
                accumulator.append(contentsOf: bytes)
            case let .responseBodyChunk(id, bytes, _) where response && id == requestID:
                accumulator.append(contentsOf: bytes)
            default:
                break
            }
        }
    }

    func totalByteCount(response: Bool) -> Int {
        events.reduce(into: 0) { accumulator, event in
            switch event {
            case let .requestBodyChunk(_, _, count) where !response: accumulator += count
            case let .responseBodyChunk(_, _, count) where response: accumulator += count
            default: break
            }
        }
    }

    var truncated: Bool {
        events.contains {
            switch $0 {
            case .requestEnd(_, let truncated), .responseEnd(_, let truncated): return truncated
            default: return false
            }
        }
    }
}

private final class LockedBodySink: CaptureEventSink, @unchecked Sendable {
    private let lock = NIOLock()
    private var body: [UInt8] = []
    private var byteCount = 0
    private var truncated = false

    func receive(_ event: CaptureEvent) {
        lock.withLock {
            switch event {
            case let .responseBodyChunk(_, bytes, count):
                body.append(contentsOf: bytes)
                byteCount += count
            case .responseEnd(_, let isTruncated):
                truncated = truncated || isTruncated
            default:
                break
            }
        }
    }

    var responseBody: [UInt8] { lock.withLock { body } }
    var totalResponseByteCount: Int { lock.withLock { byteCount } }
    var sawTruncation: Bool { lock.withLock { truncated } }
}

final class CaptureBodyTests: XCTestCase {
    // MARK: HTTP/1.1

    func testHTTP1RequestBodyFullyCapturedWhenUnderLimit() throws {
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .request,
            authority: "example.com:443",
            correlator: HTTP1ExchangeCorrelator(),
            sink: sink,
            captureBodyLimit: 1024
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(byteBuffer("POST /x HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"))

        let id = try XCTUnwrap(firstRequestID(sink))
        XCTAssertEqual(sink.body(for: id, response: false), Array("hello".utf8))
        XCTAssertEqual(sink.totalByteCount(response: false), 5)
        XCTAssertFalse(sink.truncated)
    }

    func testHTTP1ResponseBodyTruncatedAtLimitButByteCountIsFullSize() throws {
        let correlator = HTTP1ExchangeCorrelator()
        correlator.enqueue(id: UUID(), method: "GET")
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .response,
            authority: "example.com:443",
            correlator: correlator,
            sink: sink,
            captureBodyLimit: 4
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(byteBuffer("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789"))

        let id = try XCTUnwrap(firstResponseID(sink))
        XCTAssertEqual(sink.body(for: id, response: true), Array("0123".utf8))
        XCTAssertEqual(sink.totalByteCount(response: true), 10)
        XCTAssertTrue(sink.truncated)
    }

    func testHTTP1LimitZeroCapturesNoBytes() throws {
        let correlator = HTTP1ExchangeCorrelator()
        correlator.enqueue(id: UUID(), method: "GET")
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .response,
            authority: "example.com:443",
            correlator: correlator,
            sink: sink
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(byteBuffer("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody"))

        let id = try XCTUnwrap(firstResponseID(sink))
        XCTAssertEqual(sink.body(for: id, response: true), [])
        XCTAssertEqual(sink.totalByteCount(response: true), 4)
        XCTAssertFalse(sink.truncated)
    }

    func testHTTP1BodySplitAcrossReadsConcatenatesUpToLimit() throws {
        let correlator = HTTP1ExchangeCorrelator()
        correlator.enqueue(id: UUID(), method: "GET")
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP1CaptureTapHandler(
            direction: .response,
            authority: "example.com:443",
            correlator: correlator,
            sink: sink,
            captureBodyLimit: 6
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(byteBuffer("HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nab"))
        try channel.writeInbound(byteBuffer("cdefgh"))

        let id = try XCTUnwrap(firstResponseID(sink))
        XCTAssertEqual(sink.body(for: id, response: true), Array("abcdef".utf8))
        XCTAssertEqual(sink.totalByteCount(response: true), 8)
        XCTAssertTrue(sink.truncated)
    }

    // MARK: HTTP/2

    func testHTTP2RequestBodyFullyCapturedWhenUnderLimit() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .request,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink,
            captureBodyLimit: 1024
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(headersPayload(method: "POST", path: "/x"))
        try channel.writeInbound(dataPayload("hello", endStream: true))

        XCTAssertEqual(sink.body(for: requestID, response: false), Array("hello".utf8))
        XCTAssertEqual(sink.totalByteCount(response: false), 5)
        XCTAssertFalse(sink.truncated)
    }

    func testHTTP2ResponseBodyTruncatedAcrossFramesButByteCountIsFullSize() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .response,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink,
            captureBodyLimit: 4
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(statusPayload(200))
        try channel.writeInbound(dataPayload("012", endStream: false))
        try channel.writeInbound(dataPayload("3456789", endStream: true))

        XCTAssertEqual(sink.body(for: requestID, response: true), Array("0123".utf8))
        XCTAssertEqual(sink.totalByteCount(response: true), 10)
        XCTAssertTrue(sink.truncated)
    }

    func testHTTP2LimitZeroCapturesNoBytes() throws {
        let requestID = UUID()
        let sink = RecordingSink()
        let channel = EmbeddedChannel(handler: HTTP2CaptureTapHandler(
            direction: .response,
            requestID: requestID,
            authority: "example.com:443",
            sink: sink
        ))
        defer { _ = try? channel.finish() }

        try channel.writeInbound(statusPayload(200))
        try channel.writeInbound(dataPayload("body", endStream: true))

        XCTAssertEqual(sink.body(for: requestID, response: true), [])
        XCTAssertEqual(sink.totalByteCount(response: true), 4)
        XCTAssertFalse(sink.truncated)
    }

    // MARK: Integration — forward-untouched while capturing bounded

    func testProxyDeliversFullBodyDownstreamWhileCapturingBoundedSlice() async throws {
        let traffic = MultiThreadedEventLoopGroup.singleton
        let bodySize = 200_000
        let captureLimit = 1024

        let origin = try TLSOriginServer(group: traffic, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }

        let mitmCA = try CertificateAuthority()
        let sink = LockedBodySink()
        let proxy = ProxyServer(
            certificateAuthority: mitmCA,
            sink: sink,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: captureLimit
        )
        let proxyPort = try await proxy.start(port: 0)
        defer { Task { try? await proxy.stop() } }

        let originHost = origin.hostname
        let originPort = origin.localPort
        let mitmPEM = mitmCA.caCertificatePEM
        let received = try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result {
                    try ProxyTestClient(group: traffic).fetch(
                        proxyPort: proxyPort,
                        originHost: originHost,
                        originPort: originPort,
                        mitmCACertificatePEM: mitmPEM,
                        alpn: "http/1.1"
                    )
                })
            }
        }

        XCTAssertEqual(received, bodySize, "downstream must receive the full body untouched")

        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline, sink.totalResponseByteCount < bodySize {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertEqual(sink.totalResponseByteCount, bodySize, "byteCount must report the full body size")
        XCTAssertEqual(sink.responseBody.count, captureLimit, "captured bytes must be bounded by the limit")
        XCTAssertEqual(sink.responseBody, [UInt8](repeating: 0x41, count: captureLimit))
        XCTAssertTrue(sink.sawTruncation, "truncation must be signalled when the body exceeds the limit")
    }

    // MARK: Helpers

    private func byteBuffer(_ string: String) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf8.count)
        buffer.writeString(string)
        return buffer
    }

    private func headersPayload(method: String, path: String) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":method", value: method)
        headers.add(name: ":path", value: path)
        headers.add(name: ":scheme", value: "https")
        headers.add(name: ":authority", value: "example.com")
        return .headers(.init(headers: headers))
    }

    private func statusPayload(_ status: Int) -> HTTP2Frame.FramePayload {
        var headers = HPACKHeaders()
        headers.add(name: ":status", value: String(status))
        return .headers(.init(headers: headers))
    }

    private func dataPayload(_ string: String, endStream: Bool) -> HTTP2Frame.FramePayload {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf8.count)
        buffer.writeString(string)
        return .data(.init(data: .byteBuffer(buffer), endStream: endStream))
    }

    private func firstRequestID(_ sink: RecordingSink) -> UUID? {
        sink.events.compactMap { event -> UUID? in
            if case let .requestHead(head) = event { return head.id }
            return nil
        }.first
    }

    private func firstResponseID(_ sink: RecordingSink) -> UUID? {
        sink.events.compactMap { event -> UUID? in
            if case let .responseHead(head) = event { return head.requestID }
            return nil
        }.first
    }
}

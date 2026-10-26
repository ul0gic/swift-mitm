import Foundation
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicHTTP2MalformedTests: XCTestCase {
    func testDuplicateMethodResetsOnlyMalformedStream() async throws {
        try await malformed { $0 + [(":method", "POST")] }
    }

    func testConnectionSpecificFieldResetsOnlyMalformedStream() async throws {
        try await malformed { $0 + [("connection", "keep-alive")] }
    }

    func testTransferEncodingResetsOnlyMalformedStream() async throws {
        try await malformed { $0 + [("transfer-encoding", "chunked")] }
    }

    func testConflictingContentLengthResetsOnlyMalformedStream() async throws {
        try await malformed { $0 + [("content-length", "0"), ("content-length", "1")] }
    }

    private func malformed(_ transform: ([(String, String)]) -> [(String, String)]) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Release022HTTP2Origin(group: group)
        let authority = try CertificateAuthority.generate().authority
        let sink = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: authority, sink: sink, group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.authority.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true)
        )
        var client: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let peer = ProxyTestClient(group: group)
            let channel = try peer.openTunnel(proxyPort: port, originHost: "localhost", originPort: origin.port)
            client = channel
            let received = Release022WireObservation()
            try phase4BoundedWait(channel.eventLoop.submit {
                try channel.pipeline.syncOperations.addHandler(Release022WireHandler(received))
            })
            let tls = try peer.installTLS(
                on: channel, serverHostname: "localhost", mitmCACertificatePEM: authority.caCertificatePEM,
                applicationProtocols: ["h2"]
            )
            try phase4BoundedWait(tls.installed)
            XCTAssertEqual(try phase4BoundedWait(tls.handshake), "h2")
            let bytes = try malformedAndControlBytes(port: origin.port, transform: transform)
            try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: bytes)))
            let control = try origin.stream(0)
            try control.wait { !$0.isEmpty }
            try assertMalformedStreamIsolation(control: control, received: received, sink: sink)
            try await channel.close().get()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
        } catch {
            if let client, client.isActive {
                try? await client.close().get()
            }
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }


    private func assertMalformedStreamIsolation(
        control: Release022H2Stream, received: Release022WireObservation, sink: Release022Events
    ) throws {
        let paths = control.frames.compactMap { frame -> String? in
            if case .headers(let headers) = frame {
                headers.headers.first(name: ":path")
            } else {
                nil
            }
        }
        XCTAssertEqual(paths, ["/control"])
        try control.write(.headers(.init(headers: HPACKHeaders([(":status", "204")]), endStream: true)))
        _ = try received.wait { bytes in
            let frames = self.frames(bytes)
            return frames.contains { $0.type == 3 && $0.stream == 1 }
                && frames.contains { $0.type == 1 && $0.stream == 3 && $0.flags & 1 != 0 }
        }
        let events = try sink.wait { $0.contains {
            if case .responseEnd = $0 {
                true
            } else {
                false
            }
        } }
        let requests = events.compactMap { event -> CapturedRequestHead? in
            if case .requestHead(let head) = event {
                head
            } else {
                nil
            }
        }
        XCTAssertEqual(requests.map(\.path), ["/control"])
        XCTAssertFalse(self.frames(received.bytes).contains { $0.type == 7 })
    }

    private func malformedAndControlBytes(
        port: Int, transform: ([(String, String)]) -> [(String, String)]
    ) throws -> [UInt8] {
        let base = [(":method", "GET"), (":scheme", "https"), (":authority", "localhost:\(port)"), (":path", "/bad")]
        var encoder = HPACKEncoder(allocator: ByteBufferAllocator(), useHuffmanEncoding: false)
        var bad = ByteBufferAllocator().buffer(capacity: 256)
        try encoder.encode(headers: HPACKHeaders(transform(base)), to: &bad)
        var good = ByteBufferAllocator().buffer(capacity: 256)
        try encoder.encode(headers: HPACKHeaders([(":method", "GET"), (":scheme", "https"), (":authority", "localhost:\(port)"), (":path", "/control")]), to: &good)
        let preface = Array("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".utf8)
        return preface + frame(type: 4, flags: 0, stream: 0, payload: [])
            + frame(type: 1, flags: 5, stream: 1, payload: Array(bad.readableBytesView))
            + frame(type: 1, flags: 5, stream: 3, payload: Array(good.readableBytesView))
    }

    private func frame(type: UInt8, flags: UInt8, stream: UInt8, payload: [UInt8]) -> [UInt8] {
        [UInt8((payload.count >> 16) & 255), UInt8((payload.count >> 8) & 255), UInt8(payload.count & 255), type, flags, 0, 0, 0, stream] + payload
    }

    private struct FrameHeader {
        let type: UInt8
        let flags: UInt8
        let stream: Int
    }

    private func frames(_ bytes: [UInt8]) -> [FrameHeader] {
        var offset = 0
        var frames: [FrameHeader] = []
        while offset + 9 <= bytes.count {
            let length = Int(bytes[offset]) << 16 | Int(bytes[offset + 1]) << 8 | Int(bytes[offset + 2])
            guard offset + 9 + length <= bytes.count else { break }
            let stream = Int(bytes[offset + 5] & 127) << 24 | Int(bytes[offset + 6]) << 16
                | Int(bytes[offset + 7]) << 8 | Int(bytes[offset + 8])
            frames.append(FrameHeader(type: bytes[offset + 3], flags: bytes[offset + 4], stream: stream))
            offset += 9 + length
        }
        return frames
    }
}

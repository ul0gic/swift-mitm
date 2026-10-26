import Foundation
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

import SwiftMITM

final class Release022PublicHTTP2StreamingTests: XCTestCase {
    func testGRPCUnaryPreservesEnvelopeBinaryMetadataAndTrailers() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 3, 1, 2, 3]], responseMessages: [[0, 0, 0, 0, 2, 4, 5]]
        )
    }

    func testGRPCClientStreamingPreservesEveryEnvelope() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 1, 1], [0, 0, 0, 0, 1, 2]], responseMessages: [[0, 0, 0, 0, 1, 3]]
        )
    }

    func testGRPCServerStreamingPreservesCompressedRawPayload() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 1, 1]],
            responseMessages: [[1, 0, 0, 0, 2, 0xCA, 0xFE], [1, 0, 0, 0, 1, 0xFF]]
        )
    }

    func testGRPCBidirectionalStreamingReleasesResponsesBeforeRequestEnd() async throws {
        try await grpc(
            requestMessages: [[0, 0, 0, 0, 1, 1], [0, 0, 0, 0, 1, 2]],
            responseMessages: [[0, 0, 0, 0, 1, 3], [0, 0, 0, 0, 1, 4]], interleave: true
        )
    }

    func testGRPCTrailersOnlyFailureHasNoFabricatedBody() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(headers: self.requestHeaders(port: origin.port), endStream: true)))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([
                (":status", "200"), ("content-type", "application/grpc"),
                ("grpc-status", "7"), ("grpc-message", "permission%20denied")
            ]), endStream: true)))
            try client.wait { $0.contains {
                if case .headers(let h) = $0 {
                    h.endStream
                } else {
                    false
                }
            } }
            _ = try sink.wait { $0.contains {
                if case .responseEnd = $0 {
                    true
                } else {
                    false
                }
            } }
            let heads = sink.events.compactMap { event -> CapturedResponseHead? in
                if case .responseHead(let head) = event {
                    head
                } else {
                    nil
                }
            }
            XCTAssertEqual(heads.first?.headers.first { $0.name == "grpc-status" }?.value, "7")
            XCTAssertFalse(sink.events.contains {
                if case .responseBodyChunk = $0 {
                    true
                } else {
                    false
                }
            })
            XCTAssertFalse(sink.events.contains {
                if case .streamError = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    func testCancelledStreamDoesNotTerminateConcurrentGRPCControl() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let cancelled = try self.newStream(multiplexer)
            try cancelled.write(.headers(.init(headers: self.requestHeaders(port: origin.port, path: "/rpc/cancel"))))
            let first = try origin.stream(0)
            try first.wait { !$0.isEmpty }
            let control = try self.newStream(multiplexer)
            try control.write(.headers(.init(
                headers: self.requestHeaders(port: origin.port, path: "/rpc/control"), endStream: true
            )))
            let second = try origin.stream(1)
            try second.wait { !$0.isEmpty }
            try cancelled.write(.rstStream(.cancel))
            try second.write(.headers(.init(headers: HPACKHeaders([(":status", "200"), ("content-type", "application/grpc")]))))
            let bytes: [UInt8] = [0, 0, 0, 0, 1, 42]
            try second.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: bytes)))))
            try second.write(.headers(.init(headers: HPACKHeaders([("grpc-status", "0")]), endStream: true)))
            try control.wait { $0.contains {
                if case .headers(let h) = $0 {
                    h.endStream
                } else {
                    false
                }
            } }
            XCTAssertEqual(self.dataBytes(control.frames), bytes)
            _ = try sink.wait { events in events.contains {
                if case .responseEnd = $0 {
                    true
                } else {
                    false
                }
            } }
            let requests = sink.events.compactMap { event -> CapturedRequestHead? in
                if case .requestHead(let head) = event {
                    head
                } else {
                    nil
                }
            }
            let controlID = try XCTUnwrap(requests.first { $0.path == "/rpc/control" }?.id)
            XCTAssertFalse(sink.events.contains {
                if case .streamError(let id, _) = $0 {
                    id == controlID
                } else {
                    false
                }
            })
        }
    }

    func testLateWindowUpdateAfterResponseEndKeepsRequestLegOpen() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(headers: self.requestHeaders(port: origin.port))))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([(":status", "200")]))))
            try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(string: "done")), endStream: true)))
            try client.wait { $0.contains {
                if case .data(let data) = $0 {
                    data.endStream
                } else {
                    false
                }
            } }
            _ = try sink.wait { $0.contains {
                if case .responseEnd = $0 {
                    true
                } else {
                    false
                }
            } }
            try client.write(.windowUpdate(windowSizeIncrement: 1024))
            try client.write(.data(.init(data: .byteBuffer(ByteBuffer(string: "still-open")), endStream: true)))
            try upstream.wait { self.dataBytes($0) == Array("still-open".utf8) }
            let events = try sink.wait { $0.contains {
                if case .requestEnd = $0 {
                    true
                } else {
                    false
                }
            } }
            XCTAssertFalse(events.contains {
                if case .streamError = $0 {
                    true
                } else {
                    false
                }
            })
            XCTAssertFalse(upstream.frames.contains {
                if case .windowUpdate = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    func testHTTP2SSEChunksArriveBeforeNextBarrierAndEndStream() async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(headers: self.requestHeaders(port: origin.port, path: "/events"), endStream: true)))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([(":status", "200"), ("content-type", "text/event-stream")]))))
            let chunks: [[UInt8]] = [Array(": heartbeat\n\ndata: ".utf8) + [0xE2], [0x82, 0xAC, 10, 10]]
            var released: [UInt8] = []
            for chunk in chunks {
                try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: chunk)))))
                released += chunk
                let expected = released
                try client.wait { self.dataBytes($0) == expected }
                _ = try sink.wait { self.responseBytes($0) == expected }
                XCTAssertFalse(sink.events.contains {
                    if case .responseEnd = $0 {
                        true
                    } else {
                        false
                    }
                })
            }
            try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer()), endStream: true)))
            _ = try sink.wait { $0.contains {
                if case .responseEnd = $0 {
                    true
                } else {
                    false
                }
            } }
        }
    }

    private func grpc(
        requestMessages: [[UInt8]], responseMessages: [[UInt8]], interleave: Bool = false
    ) async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(headers: self.requestHeaders(port: origin.port))))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([
                (":status", "200"), ("content-type", "application/grpc"),
                ("grpc-encoding", "gzip"), ("server-bin", "AP8=")
            ]))))
            for (index, bytes) in requestMessages.enumerated() {
                try client.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: bytes)))))
                let expected = requestMessages.prefix(index + 1).flatMap(\.self)
                try upstream.wait { self.dataBytes($0) == expected }
                if interleave, index < responseMessages.count {
                    try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: responseMessages[index])))))
                    let expectedResponse = responseMessages.prefix(index + 1).flatMap(\.self)
                    try client.wait { self.dataBytes($0) == expectedResponse }
                    _ = try sink.wait { self.responseBytes($0) == expectedResponse }
                    XCTAssertFalse(sink.events.contains {
                        if case .requestEnd = $0 {
                            true
                        } else {
                            false
                        }
                    })
                }
            }
            try client.write(.headers(.init(headers: HPACKHeaders([("client-bin", "AQI=")]), endStream: true)))
            try upstream.wait { $0.contains {
                if case .headers(let h) = $0 {
                    h.endStream
                } else {
                    false
                }
            } }
            let released = interleave ? min(requestMessages.count, responseMessages.count) : 0
            for (index, bytes) in responseMessages.enumerated().dropFirst(released) {
                try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: bytes)))))
                let expected = responseMessages.prefix(index + 1).flatMap(\.self)
                try client.wait { self.dataBytes($0) == expected }
                _ = try sink.wait { self.responseBytes($0) == expected }
                XCTAssertFalse(sink.events.contains {
                    if case .responseEnd = $0 {
                        true
                    } else {
                        false
                    }
                })
            }
            try upstream.write(.headers(.init(headers: HPACKHeaders([("grpc-status", "0"), ("grpc-message", "ok"), ("trailer-bin", "//4=")]), endStream: true)))
            try client.wait { $0.contains {
                if case .headers(let h) = $0 {
                    h.endStream
                } else {
                    false
                }
            } }
            let events = try sink.wait { $0.contains {
                if case .responseEnd = $0 {
                    true
                } else {
                    false
                }
            } }
            XCTAssertEqual(self.dataBytes(client.frames), responseMessages.flatMap(\.self))
            XCTAssertEqual(self.responseBytes(events), responseMessages.flatMap(\.self))
            let requestBytes = events.flatMap { event -> [UInt8] in
                if case .requestBodyChunk(_, let bytes, _) = event {
                    bytes
                } else {
                    []
                }
            }
            XCTAssertEqual(requestBytes, requestMessages.flatMap(\.self))
            let trailers = events.flatMap { event -> [HTTPHeaderField] in
                if case .responseTrailers(_, let headers) = event {
                    headers
                } else {
                    []
                }
            }
            XCTAssertEqual(trailers.first { $0.name == "grpc-status" }?.value, "0")
            XCTAssertEqual(trailers.first { $0.name == "trailer-bin" }?.value, "//4=")
            XCTAssertFalse(events.contains {
                if case .streamError = $0 {
                    true
                } else {
                    false
                }
            })
        }
    }

    private func requestHeaders(port: Int, path: String = "/rpc/Exchange") -> HPACKHeaders {
        HPACKHeaders([
            (":method", "POST"), (":scheme", "https"), (":authority", "localhost:\(port)"),
            (":path", path), ("content-type", "application/grpc"), ("te", "trailers"), ("client-bin", "AQI=")
        ])
    }

    private func newStream(_ multiplexer: NIOHTTP2Handler.StreamMultiplexer) throws -> Release022H2Stream {
        let observation = Release022H2Stream()
        _ = try phase4BoundedWait(multiplexer.createStreamChannel { channel in
            channel.eventLoop.makeCompletedFuture { try channel.pipeline.syncOperations.addHandler(observation) }
        })
        return observation
    }

    private func dataBytes(_ frames: [HTTP2Frame.FramePayload]) -> [UInt8] {
        frames.flatMap { frame in
            if case .data(let data) = frame, case .byteBuffer(let buffer) = data.data {
                Array(buffer.readableBytesView)
            } else {
                []
            }
        }
    }

    private func responseBytes(_ events: [CaptureEvent]) -> [UInt8] {
        events.flatMap {
            if case .responseBodyChunk(_, let bytes, _) = $0 {
                bytes
            } else {
                []
            }
        }
    }

    private func withHTTP2(
        body: (Release022HTTP2Origin, NIOHTTP2Handler.StreamMultiplexer, Release022Events) throws -> Void
    ) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Release022HTTP2Origin(group: group)
        let authority = try CertificateAuthority.generate().authority
        let sink = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: authority, sink: sink, group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.authority.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true), captureBodyLimit: 65_536
        )
        var client: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let peer = ProxyTestClient(group: group)
            let channel = try peer.openTunnel(proxyPort: port, originHost: "localhost", originPort: origin.port)
            client = channel
            let tls = try peer.installTLS(
                on: channel, serverHostname: "localhost", mitmCACertificatePEM: authority.caCertificatePEM,
                applicationProtocols: ["h2"], configuresHTTP2: true
            )
            try phase4BoundedWait(tls.installed)
            XCTAssertEqual(try phase4BoundedWait(tls.handshake), "h2")
            let future = try XCTUnwrap(tls.multiplexer)
            let multiplexer = try XCTUnwrap(try phase4BoundedWait(future))
            try body(origin, multiplexer, sink)
            if channel.isActive {
                try await channel.close().get()
            }
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
}

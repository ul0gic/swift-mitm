import Foundation
import NIOCore
import NIOHPACK
import NIOHTTP2
import NIOPosix
import XCTest

import SwiftMITM

extension Release022PublicHTTP2StreamingTests {
    func assertControlStreamHasNoError(_ sink: Release022Events) throws {
        let requests = sink.events.compactMap { event -> CapturedRequestHead? in
            if case let .requestHead(head) = event {
                head
            } else {
                nil
            }
        }
        let controlID = try XCTUnwrap(requests.first { $0.path == "/rpc/control" }?.id)
        XCTAssertFalse(sink.events.contains {
            if case let .streamError(id, _) = $0 {
                id == controlID
            } else {
                false
            }
        })
    }

    func grpc(
        requestMessages: [[UInt8]],
        responseMessages: [[UInt8]],
        interleave: Bool = false
    ) async throws {
        try await withHTTP2 { origin, multiplexer, sink in
            let client = try self.newStream(multiplexer)
            try client.write(.headers(.init(headers: self.requestHeaders(port: origin.port))))
            let upstream = try origin.stream(0)
            try upstream.wait { !$0.isEmpty }
            try upstream.write(.headers(.init(headers: HPACKHeaders([
                (":status", "200"),
                ("content-type", "application/grpc"),
                ("grpc-encoding", "gzip"),
                ("server-bin", "AP8=")
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
                if case let .headers(headers) = $0 {
                    headers.endStream
                } else {
                    false
                }
            } }
            let released = interleave ? min(requestMessages.count, responseMessages.count) : 0
            let events = try self.finishGRPCResponse(
                client: client,
                upstream: upstream,
                sink: sink,
                responseMessages: responseMessages,
                released: released
            )
            self.assertGRPCExchange(
                client: client,
                events: events,
                requestMessages: requestMessages,
                responseMessages: responseMessages
            )
        }
    }

    func finishGRPCResponse(
        client: Release022H2Stream,
        upstream: Release022H2Stream,
        sink: Release022Events,
        responseMessages: [[UInt8]],
        released: Int
    ) throws -> [CaptureEvent] {
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
        try upstream.write(.headers(.init(
            headers: HPACKHeaders([("grpc-status", "0"), ("grpc-message", "ok"), ("trailer-bin", "//4=")]),
            endStream: true
        )))
        try client.wait { $0.contains {
            if case let .headers(headers) = $0 {
                headers.endStream
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
        return events
    }

    func assertGRPCExchange(
        client: Release022H2Stream,
        events: [CaptureEvent],
        requestMessages: [[UInt8]],
        responseMessages: [[UInt8]]
    ) {
        XCTAssertEqual(self.dataBytes(client.frames), responseMessages.flatMap(\.self))
        XCTAssertEqual(self.responseBytes(events), responseMessages.flatMap(\.self))
        let requestBytes = events.flatMap { event -> [UInt8] in
            if case let .requestBodyChunk(_, bytes, _) = event {
                bytes
            } else {
                []
            }
        }
        XCTAssertEqual(requestBytes, requestMessages.flatMap(\.self))
        let trailers = events.flatMap { event -> [HTTPHeaderField] in
            if case let .responseTrailers(_, headers) = event {
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

    func requestHeaders(port: Int, path: String = "/rpc/Exchange") -> HPACKHeaders {
        HPACKHeaders([
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "localhost:\(port)"),
            (":path", path),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
            ("client-bin", "AQI=")
        ])
    }

    func newStream(_ multiplexer: NIOHTTP2Handler.StreamMultiplexer) throws -> Release022H2Stream {
        let observation = Release022H2Stream()
        _ = try phase4BoundedWait(multiplexer.createStreamChannel { channel in
            channel.eventLoop.makeCompletedFuture { try channel.pipeline.syncOperations.addHandler(observation) }
        })
        return observation
    }

    func dataBytes(_ frames: [HTTP2Frame.FramePayload]) -> [UInt8] {
        frames.flatMap { frame in
            if case let .data(data) = frame, case let .byteBuffer(buffer) = data.data {
                Array(buffer.readableBytesView)
            } else {
                []
            }
        }
    }

    func responseBytes(_ events: [CaptureEvent]) -> [UInt8] {
        events.flatMap {
            if case let .responseBodyChunk(_, bytes, _) = $0 {
                bytes
            } else {
                []
            }
        }
    }

    func withHTTP2(
        body: (Release022HTTP2Origin, NIOHTTP2Handler.StreamMultiplexer, Release022Events) throws -> Void
    ) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Release022HTTP2Origin(group: group)
        let authority = try CertificateAuthority.generate().authority
        let sink = Release022Events()
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [origin.authority.caCertificatePEM]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 65_536
        )
        var client: Channel?
        do {
            try origin.start()
            let port = try await proxy.start(port: 0)
            let peer = ProxyTestClient(group: group)
            let channel = try peer.openTunnel(proxyPort: port, originHost: "localhost", originPort: origin.port)
            client = channel
            let tls = try peer.installTLS(
                on: channel,
                serverHostname: "localhost",
                mitmCACertificatePEM: authority.caCertificatePEM,
                applicationProtocols: ["h2"],
                configuresHTTP2: true
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

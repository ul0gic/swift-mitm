import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

func release022WithWire(
    transparent: Bool = false,
    captureLimit: Int = 65_536,
    ipv6: Bool = false,
    egressPolicy: EgressPolicy = .init(allowInternal: true),
    body: (Release022WireOrigin, Channel, Release022WireObservation, Release022Events) throws -> Void
) async throws {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
    let origin = Release022WireOrigin(group: group)
    let sink = Release022Events()
    let ingress: ProxyIngress = transparent
        ? .trustedProxyV2(try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))) : .explicitHTTP
    let proxy = ProxyServer(
        certificateAuthority: try CertificateAuthority.generate().authority,
        sink: sink,
        group: group,
        egressPolicy: egressPolicy,
        ingress: ingress,
        captureBodyLimit: captureLimit
    )
    var client: Channel?
    do {
        try origin.start(host: ipv6 ? "::1" : "127.0.0.1")
        let port = try await proxy.start(port: 0)
        let received = Release022WireObservation()
        let connection = try release022WireClient(group: group, port: port, observation: received)
        client = connection
        if transparent {
            try release022SendProxyV2(on: connection, originPort: origin.port, ipv6: ipv6)
        }
        try body(origin, connection, received, sink)
        if connection.isActive {
            try await connection.close().get()
        }
        try await proxy.stop()
        origin.stop()
        try await group.shutdownGracefully()
    } catch {
        if let client {
            try? await client.close().get()
        }
        try? await proxy.stop()
        origin.stop()
        try? await group.shutdownGracefully()
        throw error
    }
}

private func release022SendProxyV2(on connection: Channel, originPort: Int, ipv6: Bool) throws {
    let header: Phase4ProxyV2Header
    if ipv6 {
        let loopback = [UInt8](repeating: 0, count: 15) + [1]
        header = .ipv6(
            source: loopback,
            destination: loopback,
            sourcePort: 50_000,
            destinationPort: originPort
        )
    } else {
        header = .ipv4(
            source: [192, 0, 2, 1],
            destination: [127, 0, 0, 1],
            sourcePort: 50_000,
            destinationPort: originPort
        )
    }
    try phase4BoundedWait(connection.writeAndFlush(ByteBuffer(bytes: header.bytes)))
}

func release022Requests(_ events: [CaptureEvent]) -> [CapturedRequestHead] {
    events.compactMap {
        if case let .requestHead(head) = $0 {
            head
        } else {
            nil
        }
    }
}

func release022Errors(_ events: [CaptureEvent]) -> [UUID] {
    events.compactMap {
        if case let .streamError(id, _) = $0 {
            id
        } else {
            nil
        }
    }
}

func release022ResponseEnds(_ events: [CaptureEvent]) -> [UUID] {
    events.compactMap {
        if case let .responseEnd(id, _) = $0 {
            id
        } else {
            nil
        }
    }
}

func release022ResponseBodyBytes(_ events: [CaptureEvent]) -> [UInt8] {
    events.flatMap { event -> [UInt8] in
        if case let .responseBodyChunk(_, bytes, _) = event {
            bytes
        } else {
            []
        }
    }
}

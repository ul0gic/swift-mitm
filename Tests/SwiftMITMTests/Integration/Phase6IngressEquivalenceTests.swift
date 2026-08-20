import Foundation
import NIOCore
import NIOPosix
import XCTest

import SwiftMITM

final class Phase6IngressEquivalenceTests: XCTestCase {
    func testExplicitAndTransparentHTTP11CaptureIsEquivalentAfterTargetResolution() async throws {
        let explicit = try await performHTTPExchange(alpn: "http/1.1", ingress: .explicit)
        let transparent = try await performHTTPExchange(alpn: "http/1.1", ingress: .transparent)

        XCTAssertEqual(explicit, transparent)
    }

    func testExplicitAndTransparentHTTP2CaptureIsEquivalentAfterTargetResolution() async throws {
        let explicit = try await performHTTPExchange(alpn: "h2", ingress: .explicit)
        let transparent = try await performHTTPExchange(alpn: "h2", ingress: .transparent)

        XCTAssertEqual(explicit, transparent)
    }

    func testExplicitAndTransparentHTTP11WebSocketCaptureIsEquivalentAfterTargetResolution() async throws {
        let explicit = try await performHTTP11WebSocketExchange(ingress: .explicit)
        let transparent = try await performHTTP11WebSocketExchange(ingress: .transparent)

        XCTAssertEqual(explicit, transparent)
    }

    func testExplicitAndTransparentRFC8441CaptureIsEquivalentAfterTargetResolution() async throws {
        let explicit = try await performRFC8441Exchange(ingress: .explicit)
        let transparent = try await performRFC8441Exchange(ingress: .transparent)

        XCTAssertEqual(explicit, transparent)
    }

    private func performHTTPExchange(
        alpn: String,
        ingress: Phase6TestIngress
    ) async throws -> Phase6HTTPObservation {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try TLSOriginServer(group: group, bodySize: 32, applicationProtocols: [alpn])
        let authority = try CertificateAuthority.generate().authority
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(ingress, group, origin.caCertificatePEM, authority, sink, 8)
        var adapter: Phase6IngressAdapter?

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let entryPort = try startAdapterIfNeeded(
                ingress: ingress,
                group: group,
                proxyPort: proxyPort,
                originPort: origin.localPort,
                storage: &adapter
            )
            let originHost = origin.hostname
            let originPort = origin.localPort
            let authorityPEM = authority.caCertificatePEM
            let bodyBytes = try await runBlocking {
                try ProxyTestClient(group: group).fetch(
                    proxyPort: entryPort,
                    originHost: originHost,
                    originPort: originPort,
                    mitmCACertificatePEM: authorityPEM,
                    alpn: alpn
                )
            }
            let observation = try makeHTTPObservation(
                sink: sink,
                bodyBytes: bodyBytes,
                ingress: ingress,
                originPort: origin.localPort
            )
            adapter?.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
            return observation
        } catch {
            adapter?.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func performHTTP11WebSocketExchange(
        ingress: Phase6TestIngress
    ) async throws -> Phase6WebSocketObservation {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try WebSocketTLSOriginServer(group: group, targetHost: "localhost")
        let authority = try CertificateAuthority.generate().authority
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(ingress, group, origin.caCertificatePEM, authority, sink, 64)
        var adapter: Phase6IngressAdapter?

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let entryPort = try startAdapterIfNeeded(
                ingress: ingress,
                group: group,
                proxyPort: proxyPort,
                originPort: origin.localPort,
                storage: &adapter
            )
            let client = try await runBlocking {
                try WebSocketProxyClient(group: group).exchange(
                    proxyPort: entryPort,
                    originHost: "localhost",
                    originPort: origin.localPort,
                    mitmCACertificatePEM: authority.caCertificatePEM
                )
            }
            let originResult = try await runBlocking { try origin.waitForResult() }
            let observation = try makeHTTP11WebSocketObservation(
                sink: sink,
                client: client,
                origin: originResult,
                ingress: ingress,
                originPort: origin.localPort
            )
            adapter?.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
            return observation
        } catch {
            adapter?.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func performRFC8441Exchange(
        ingress: Phase6TestIngress
    ) async throws -> Phase6WebSocketObservation {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let origin = try Phase3TLSHTTP2WebSocketOrigin(group: group)
        let authority = try CertificateAuthority.generate().authority
        let sink = Phase4CaptureSink()
        let proxy = try makeProxy(ingress, group, origin.caCertificatePEM, authority, sink, 64)
        let client = Phase3ProxyHTTP2WebSocketClient(group: group)
        var adapter: Phase6IngressAdapter?

        do {
            try origin.start()
            let proxyPort = try await proxy.start(port: 0)
            let entryPort = try startAdapterIfNeeded(
                ingress: ingress,
                group: group,
                proxyPort: proxyPort,
                originPort: origin.localPort,
                storage: &adapter
            )
            let clientResult = try await runBlocking {
                try client.exchange(
                    proxyPort: entryPort,
                    originHost: origin.hostname,
                    originPort: origin.localPort,
                    mitmCACertificatePEM: authority.caCertificatePEM
                )
            }
            let originResult = try await runBlocking { try origin.result.wait() }
            let observation = try makeRFC8441Observation(
                sink: sink,
                client: clientResult,
                origin: originResult,
                ingress: ingress,
                originPort: origin.localPort
            )
            client.stop()
            adapter?.stop()
            try await proxy.stop()
            origin.stop()
            try await group.shutdownGracefully()
            return observation
        } catch {
            client.stop()
            adapter?.stop()
            try? await proxy.stop()
            origin.stop()
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func makeProxy(
        _ ingress: Phase6TestIngress,
        _ group: EventLoopGroup,
        _ originTrustRoot: String,
        _ authority: CertificateAuthority,
        _ sink: Phase4CaptureSink,
        _ captureLimit: Int
    ) throws -> ProxyServer {
        let configuredIngress: ProxyIngress
        switch ingress {
        case .explicit:
            configuredIngress = .explicitConnect
        case .transparent:
            configuredIngress = .trustedProxyV2(try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback)))
        }
        return ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: group,
            upstreamPolicy: .init(additionalTrustRootsPEM: [originTrustRoot]),
            egressPolicy: .init(allowInternal: true),
            ingress: configuredIngress,
            captureBodyLimit: captureLimit
        )
    }

    private func startAdapterIfNeeded(
        ingress: Phase6TestIngress,
        group: EventLoopGroup,
        proxyPort: Int,
        originPort: Int,
        storage: inout Phase6IngressAdapter?
    ) throws -> Int {
        guard ingress == .transparent else { return proxyPort }
        let adapter = Phase6IngressAdapter(
            group: group,
            proxyPort: proxyPort,
            destinationPort: originPort
        )
        try adapter.start()
        storage = adapter
        return adapter.localPort
    }

    private func assertIngressBoundary(
        _ snapshot: Phase4CaptureSink.Snapshot,
        expected ingress: Phase6TestIngress,
        originPort: Int,
        applicationMethod: String
    ) throws {
        XCTAssertEqual(snapshot.requestHeads.count, 1)
        let request = try XCTUnwrap(snapshot.requestHeads.first)
        XCTAssertEqual(request.method, applicationMethod)
        XCTAssertEqual(request.target?.destination.port, originPort)
        switch ingress {
        case .explicit:
            XCTAssertEqual(request.target?.ingressProvenance, .explicitConnect)
            XCTAssertNil(request.target?.originalClient)
        case .transparent:
            XCTAssertEqual(request.target?.ingressProvenance, .trustedProxyV2)
            XCTAssertEqual(request.target?.originalClient?.address, "192.0.2.60")
            XCTAssertEqual(request.target?.originalClient?.port, 50_060)
        }
    }

    private func makeHTTPObservation(
        sink: Phase4CaptureSink,
        bodyBytes: Int,
        ingress: Phase6TestIngress,
        originPort: Int
    ) throws -> Phase6HTTPObservation {
        let snapshot = try sink.wait { $0.responseEnds.count == 1 }
        XCTAssertEqual(bodyBytes, 32)
        try assertIngressBoundary(snapshot, expected: ingress, originPort: originPort, applicationMethod: "GET")
        return try Phase6HTTPObservation(snapshot: snapshot)
    }

    private func makeHTTP11WebSocketObservation(
        sink: Phase4CaptureSink,
        client: WebSocketClientResult,
        origin: WebSocketOriginResult,
        ingress: Phase6TestIngress,
        originPort: Int
    ) throws -> Phase6WebSocketObservation {
        let snapshot = try sink.wait { $0.webSocketCloses.count == 1 }
        XCTAssertEqual(client.responseHead, WebSocketWire.responseHead)
        XCTAssertEqual(client.frames, WebSocketWire.serverFrames)
        XCTAssertEqual(origin.frames, WebSocketWire.clientFrames)
        try assertIngressBoundary(snapshot, expected: ingress, originPort: originPort, applicationMethod: "GET")
        return try Phase6WebSocketObservation(snapshot: snapshot)
    }

    private func makeRFC8441Observation(
        sink: Phase4CaptureSink,
        client: Phase3ProxyHTTP2WebSocketClientResult,
        origin: Phase3HTTP2WebSocketOriginResult,
        ingress: Phase6TestIngress,
        originPort: Int
    ) throws -> Phase6WebSocketObservation {
        let snapshot = try sink.wait { $0.webSocketCloses.count == 1 }
        XCTAssertTrue(client.downstreamExtendedConnectEnabled)
        XCTAssertEqual(client.status, 200)
        XCTAssertEqual(client.serverBytes, WebSocketWire.serverFrames)
        XCTAssertEqual(origin.clientBytes, WebSocketWire.clientFrames)
        try assertIngressBoundary(snapshot, expected: ingress, originPort: originPort, applicationMethod: "CONNECT")
        return try Phase6WebSocketObservation(snapshot: snapshot)
    }

    private func runBlocking<Value: Sendable>(
        _ operation: @escaping @Sendable () throws -> Value
    ) async throws -> Value {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                continuation.resume(with: Result { try operation() })
            }
        }
    }
}

private enum Phase6TestIngress: Equatable {
    case explicit
    case transparent
}

struct Phase6WebSocketObservation: Equatable {
    struct Frame: Equatable {
        let direction: WebSocketDirection
        let opcode: WebSocketOpcode
        let fin: Bool
        let compressed: Bool
        let bytes: [UInt8]
        let byteCount: Int
        let truncated: Bool
        let closeCode: Int?
        let closeReason: String?
    }

    let scheme: String
    let authority: String
    let method: String
    let path: String
    let version: HTTPProtocolVersion
    let requestHeaders: [HTTPHeaderField]
    let responseStatus: Int
    let responseVersion: HTTPProtocolVersion
    let responseHeaders: [HTTPHeaderField]
    let permessageDeflate: Bool
    let frames: [Frame]
    let closeCode: Int?
    let closeReason: String?
    let eventKinds: [Phase4CaptureSink.EventKind]

    init(snapshot: Phase4CaptureSink.Snapshot) throws {
        let request = try XCTUnwrap(snapshot.requestHeads.first)
        let response = try XCTUnwrap(snapshot.responseHeads.first)
        let open = try XCTUnwrap(snapshot.webSocketOpens.first)
        let close = try XCTUnwrap(snapshot.webSocketCloses.first)
        scheme = request.scheme
        authority = Self.normalizedAuthority(request.authority)
        method = request.method
        path = request.path
        version = request.version
        requestHeaders = request.headers.map { header in
            HTTPHeaderField(name: header.name, value: Self.normalizedAuthority(header.value))
        }
        responseStatus = response.status
        responseVersion = response.version
        responseHeaders = response.headers
        permessageDeflate = open.permessageDeflate
        frames = snapshot.webSocketFrames.map {
            Frame(
                direction: $0.direction,
                opcode: $0.opcode,
                fin: $0.fin,
                compressed: $0.compressed,
                bytes: $0.bytes,
                byteCount: $0.byteCount,
                truncated: $0.truncated,
                closeCode: $0.closeCode,
                closeReason: $0.closeReason
            )
        }
        closeCode = close.code
        closeReason = close.reason
        eventKinds = snapshot.eventKinds
    }

    private static func normalizedAuthority(_ authority: String) -> String {
        authority.hasPrefix("localhost:") ? "localhost:<port>" : authority
    }
}

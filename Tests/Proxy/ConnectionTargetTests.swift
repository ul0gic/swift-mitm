import NIOCore
import XCTest

@testable import SwiftMITM

final class ConnectionTargetTests: XCTestCase {
    func testHostnameConnectTargetPreservesLogicalAndTLSIdentities() throws {
        let target = try XCTUnwrap(ConnectionTarget(explicitConnectAuthority: "origin.example:8443"))

        XCTAssertEqual(target.connectionHost, "origin.example")
        XCTAssertEqual(target.port, 8443)
        XCTAssertEqual(target.logicalAuthority, "origin.example:8443")
        XCTAssertEqual(target.tlsServerName, "origin.example")
        XCTAssertEqual(target.leafIdentity, "origin.example")
        guard case .explicitConnect = target.ingressProvenance else {
            return XCTFail("expected explicit CONNECT provenance")
        }
    }

    func testIPv4ConnectTargetOmitsTLSServerName() throws {
        let target = try XCTUnwrap(ConnectionTarget(explicitConnectAuthority: "192.0.2.1:443"))

        XCTAssertEqual(target.connectionHost, "192.0.2.1")
        XCTAssertEqual(target.logicalAuthority, "192.0.2.1:443")
        XCTAssertNil(target.tlsServerName)
        XCTAssertEqual(target.leafIdentity, "192.0.2.1")
    }

    func testBracketedIPv6ConnectTargetPreservesExistingLogicalAuthority() throws {
        let target = try XCTUnwrap(ConnectionTarget(explicitConnectAuthority: "[2001:db8::1]:443"))

        XCTAssertEqual(target.connectionHost, "2001:db8::1")
        XCTAssertEqual(target.logicalAuthority, "2001:db8::1:443")
        XCTAssertNil(target.tlsServerName)
        XCTAssertEqual(target.leafIdentity, "2001:db8::1")
    }

    func testResolvedTargetAddsConnectedAddressWithoutReplacingOriginalIdentity() throws {
        let unresolved = try XCTUnwrap(ConnectionTarget(explicitConnectAuthority: "origin.example:443"))
        let address = try SocketAddress(ipAddress: "203.0.113.8", port: 443)
        let resolved = ResolvedTarget(unresolved: unresolved, connectedAddress: address)

        XCTAssertEqual(resolved.unresolved.logicalAuthority, "origin.example:443")
        XCTAssertEqual(resolved.unresolved.tlsServerName, "origin.example")
        XCTAssertEqual(resolved.connectedAddress, address)
    }

    func testProxyV2IPv4DestinationControlsRoutingAndOmitsSNI() throws {
        let source = try SocketAddress(ipAddress: "192.0.2.10", port: 12_345)
        let destination = try SocketAddress(ipAddress: "198.51.100.20", port: 8443)
        let metadata = ProxyV2Metadata(sourceAddress: source, destinationAddress: destination, tlvCount: 0)
        let target = try XCTUnwrap(ConnectionTarget(proxyV2Metadata: metadata))

        XCTAssertEqual(target.connectionHost, "198.51.100.20")
        XCTAssertEqual(target.port, 8443)
        XCTAssertEqual(target.logicalAuthority, "198.51.100.20:8443")
        XCTAssertNil(target.tlsServerName)
        XCTAssertEqual(target.leafIdentity, "198.51.100.20")
        guard case .trustedProxyV2(let originalClient) = target.ingressProvenance else {
            return XCTFail("expected trusted PROXY v2 provenance")
        }
        XCTAssertEqual(originalClient, source)
    }

    func testProxyV2IPv6DestinationUsesBracketedAuthorityWithoutSNI() throws {
        let source = try SocketAddress(ipAddress: "2001:db8::10", port: 12_345)
        let destination = try SocketAddress(ipAddress: "2001:db8::20", port: 443)
        let metadata = ProxyV2Metadata(sourceAddress: source, destinationAddress: destination, tlvCount: 0)
        let target = try XCTUnwrap(ConnectionTarget(proxyV2Metadata: metadata))

        XCTAssertEqual(target.connectionHost, "2001:db8::20")
        XCTAssertEqual(target.logicalAuthority, "[2001:db8::20]:443")
        XCTAssertNil(target.tlsServerName)
        XCTAssertEqual(target.leafIdentity, "2001:db8::20")
    }

    func testTLSServerNameRefinesIdentityWithoutChangingTransparentRouteOrProvenance() throws {
        let source = try SocketAddress(ipAddress: "192.0.2.10", port: 12_345)
        let destination = try SocketAddress(ipAddress: "198.51.100.20", port: 8443)
        let metadata = ProxyV2Metadata(sourceAddress: source, destinationAddress: destination, tlvCount: 0)
        let original = try XCTUnwrap(ConnectionTarget(proxyV2Metadata: metadata))

        let refined = original.applyingTLSMetadata(clientHelloMetadata(serverName: "api.example.com"))

        XCTAssertEqual(refined.connectionHost, "198.51.100.20")
        XCTAssertEqual(refined.port, 8443)
        XCTAssertEqual(refined.logicalAuthority, "198.51.100.20:8443")
        XCTAssertEqual(refined.tlsServerName, "api.example.com")
        XCTAssertEqual(refined.leafIdentity, "api.example.com")
        guard case .trustedProxyV2(let originalClient) = refined.ingressProvenance else {
            return XCTFail("expected trusted PROXY v2 provenance")
        }
        XCTAssertEqual(originalClient, source)
    }

    func testAbsentTLSServerNameLeavesTransparentTargetUnchanged() throws {
        let source = try SocketAddress(ipAddress: "192.0.2.10", port: 12_345)
        let destination = try SocketAddress(ipAddress: "198.51.100.20", port: 443)
        let metadata = ProxyV2Metadata(sourceAddress: source, destinationAddress: destination, tlvCount: 0)
        let original = try XCTUnwrap(ConnectionTarget(proxyV2Metadata: metadata))

        let refined = original.applyingTLSMetadata(clientHelloMetadata(serverName: nil))

        XCTAssertEqual(refined.connectionHost, original.connectionHost)
        XCTAssertEqual(refined.logicalAuthority, original.logicalAuthority)
        XCTAssertNil(refined.tlsServerName)
        XCTAssertEqual(refined.leafIdentity, original.leafIdentity)
    }

    func testInvalidConnectAuthorityCannotCreateTarget() {
        XCTAssertNil(ConnectionTarget(explicitConnectAuthority: "origin.example"))
        XCTAssertNil(ConnectionTarget(explicitConnectAuthority: "origin.example:0"))
        XCTAssertNil(ConnectionTarget(explicitConnectAuthority: "origin.example:65536"))
        XCTAssertNil(ConnectionTarget(explicitConnectAuthority: ":443"))
    }

    private func clientHelloMetadata(serverName: String?) -> ClientHelloMetadata {
        ClientHelloMetadata(
            offeredALPNProtocols: [],
            supportedALPNProtocols: [],
            hasALPNExtension: false,
            serverName: serverName,
            encryptedClientHelloDetected: false
        )
    }
}

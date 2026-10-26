import NIOCore
import XCTest

@testable import SwiftMITM

final class TrustedPeerPolicyTests: XCTestCase {
    func testPolicyRequiresOnlyValidLiteralAddressesAndCIDRs() {
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: []))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: ["localhost"]))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: ["192.0.2.1/33"]))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: ["2001:db8::1/129"]))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: ["192.0.2.1/"]))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: ["192.0.2.1/+24"]))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: ["192.0.2.1/24/8"]))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: [" 192.0.2.1"]))
        XCTAssertNil(TrustedPeerPolicy(addressesAndCIDRs: ["fe80::1%lo0"]))
    }

    func testPolicyMatchesIPv4AndIPv6Networks() throws {
        let entries = ["192.0.2.7/24", "2001:db8:abcd::/48", "203.0.113.9"]
        let policy = try XCTUnwrap(TrustedPeerPolicy(addressesAndCIDRs: entries))

        XCTAssertEqual(policy.addressesAndCIDRs, entries)
        XCTAssertTrue(policy.admits(try address("192.0.2.250")))
        XCTAssertFalse(policy.admits(try address("192.0.3.1")))
        XCTAssertTrue(policy.admits(try address("2001:db8:abcd:ffff::1")))
        XCTAssertFalse(policy.admits(try address("2001:db8:abce::1")))
        XCTAssertTrue(policy.admits(try address("203.0.113.9")))
        XCTAssertFalse(policy.admits(try address("203.0.113.10")))
    }

    func testLoopbackCoversIPv4IPv6AndMappedIPv4() throws {
        XCTAssertTrue(TrustedPeerPolicy.loopback.admits(try address("127.0.0.1")))
        XCTAssertTrue(TrustedPeerPolicy.loopback.admits(try address("127.255.255.254")))
        XCTAssertTrue(TrustedPeerPolicy.loopback.admits(try address("::1")))
        XCTAssertTrue(TrustedPeerPolicy.loopback.admits(try address("::ffff:127.0.0.1")))
        XCTAssertFalse(TrustedPeerPolicy.loopback.admits(try address("128.0.0.1")))
        XCTAssertFalse(TrustedPeerPolicy.loopback.admits(try address("::2")))
    }

    func testIngressConfigurationExposesFrozenDefaultsAndRejectsInvalidBounds() throws {
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))

        XCTAssertEqual(configuration.trustedPeers.addressesAndCIDRs, ["127.0.0.0/8", "::1/128"])
        XCTAssertEqual(configuration.proxyHeaderMaximumBytes, 4_096)
        XCTAssertEqual(configuration.proxyHeaderDeadline, .seconds(5))
        XCTAssertEqual(configuration.classificationMaximumBytes, 65_536)
        XCTAssertEqual(configuration.classificationDeadline, .seconds(1))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, proxyHeaderMaximumBytes: 15))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, proxyHeaderMaximumBytes: 65_552))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, proxyHeaderDeadline: .zero))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, classificationMaximumBytes: 0))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, classificationMaximumBytes: 65_537))
        XCTAssertNil(TrustedProxyV2Ingress(trustedPeers: .loopback, classificationDeadline: .zero))
    }

    func testIngressSelectionCarriesImmutableConfiguration() throws {
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(trustedPeers: .loopback))
        let ingress = ProxyIngress.trustedProxyV2(configuration)

        guard case .trustedProxyV2(let selected) = ingress else {
            return XCTFail("expected trusted PROXY v2 ingress")
        }
        XCTAssertEqual(selected.proxyHeaderMaximumBytes, 4_096)
    }

    func testTimeoutPolicyExposesFrozenDefaultsAndRejectsInvalidDeadlines() throws {
        let policy = try XCTUnwrap(ProxyTimeoutPolicy())

        XCTAssertEqual(policy.upstreamConnectDeadline, .seconds(10))
        XCTAssertEqual(policy.tlsHandshakeDeadline, .seconds(10))
        XCTAssertEqual(policy.http2InitialSettingsDeadline, .seconds(5))
        XCTAssertNil(ProxyTimeoutPolicy(upstreamConnectDeadline: .zero))
        XCTAssertNil(ProxyTimeoutPolicy(tlsHandshakeDeadline: .zero))
        XCTAssertNil(ProxyTimeoutPolicy(http2InitialSettingsDeadline: .zero))
    }

    private func address(_ ipAddress: String) throws -> SocketAddress {
        try SocketAddress(ipAddress: ipAddress, port: 9000)
    }
}

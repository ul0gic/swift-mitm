import XCTest

import SwiftMITM

final class ProxyIngressPublicAPITests: XCTestCase {
    func testFrozenIngressAndTimeoutConfigurationIsConsumerConstructible() throws {
        let peers = try XCTUnwrap(TrustedPeerPolicy(addressesAndCIDRs: ["192.0.2.0/24", "2001:db8::1"]))
        let configuration = try XCTUnwrap(TrustedProxyV2Ingress(
            trustedPeers: peers,
            proxyHeaderMaximumBytes: 2_048,
            proxyHeaderDeadline: .seconds(3),
            classificationMaximumBytes: 32_768,
            classificationDeadline: .milliseconds(500)
        ))
        let timeouts = try XCTUnwrap(ProxyTimeoutPolicy(
            upstreamConnectDeadline: .seconds(8),
            tlsHandshakeDeadline: .seconds(9),
            http2InitialSettingsDeadline: .seconds(4)
        ))
        let ingress = ProxyIngress.trustedProxyV2(configuration)

        XCTAssertEqual(peers.addressesAndCIDRs, ["192.0.2.0/24", "2001:db8::1"])
        XCTAssertEqual(configuration.proxyHeaderMaximumBytes, 2_048)
        XCTAssertEqual(configuration.proxyHeaderDeadline, .seconds(3))
        XCTAssertEqual(configuration.classificationMaximumBytes, 32_768)
        XCTAssertEqual(configuration.classificationDeadline, .milliseconds(500))
        XCTAssertEqual(timeouts.upstreamConnectDeadline, .seconds(8))
        XCTAssertEqual(timeouts.tlsHandshakeDeadline, .seconds(9))
        XCTAssertEqual(timeouts.http2InitialSettingsDeadline, .seconds(4))
        guard case .trustedProxyV2(let selected) = ingress else {
            return XCTFail("expected trusted PROXY v2 ingress")
        }
        XCTAssertEqual(selected.trustedPeers.addressesAndCIDRs, peers.addressesAndCIDRs)
    }
}

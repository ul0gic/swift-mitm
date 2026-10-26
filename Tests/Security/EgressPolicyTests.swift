import NIOCore
import XCTest

@testable import SwiftMITM

final class EgressPolicyTests: XCTestCase {
    private func address(_ ip: String) throws -> SocketAddress {
        try SocketAddress(ipAddress: ip, port: 443)
    }

    func testDeniesInternalV4Ranges() throws {
        let policy = EgressPolicy.default
        for ip in [
            "0.0.0.0", "10.0.0.1", "100.64.0.1", "100.127.255.254", "127.0.0.1", "169.254.169.254",
            "172.16.0.1", "172.31.255.1", "192.0.0.8", "192.0.2.1", "192.88.99.1", "192.168.1.1",
            "198.18.0.1", "198.19.255.254", "198.51.100.1", "203.0.113.1", "224.0.0.1", "255.255.255.255"
        ] {
            XCTAssertTrue(policy.denies(try address(ip)), "\(ip) must be denied")
        }
    }

    func testAllowsPublicV4() throws {
        let policy = EgressPolicy.default
        for ip in [
            "1.1.1.1", "8.8.8.8", "100.63.255.255", "100.128.0.1", "172.32.0.1", "172.15.0.1",
            "192.0.0.9", "192.0.0.10", "192.31.196.1", "192.52.193.1", "192.175.48.1", "93.184.216.34"
        ] {
            XCTAssertFalse(policy.denies(try address(ip)), "\(ip) must be allowed")
        }
    }

    func testDeniesInternalV6() throws {
        let policy = EgressPolicy.default
        for ip in [
            "::", "::1", "::ffff:127.0.0.1", "64:ff9b::127.0.0.1", "64:ff9b:1::1", "100::1",
            "100:0:0:1::1", "2001::1", "2001:2::1", "2001:db8::1", "2002::1", "3fff::1", "5f00::1",
            "fc00::1", "fd12:3456::1", "fe80::1", "fec0::1", "ff02::1"
        ] {
            XCTAssertTrue(policy.denies(try address(ip)), "\(ip) must be denied")
        }
    }

    func testAllowsPublicV6() throws {
        let policy = EgressPolicy.default
        for ip in [
            "64:ff9b::8.8.8.8", "2001:1::1", "2001:1::2", "2001:1::3", "2001:3::1",
            "2001:4:112::1", "2001:20::1", "2001:30::1", "2001:4860:4860::8888",
            "2606:4700:4700::1111", "2620:4f:8000::1"
        ] {
            XCTAssertFalse(policy.denies(try address(ip)), "\(ip) must be allowed")
        }
    }

    func testAllowInternalDisablesDenylist() throws {
        let policy = EgressPolicy(allowInternal: true)
        for ip in ["127.0.0.1", "169.254.169.254", "10.0.0.1", "::1", "fe80::1"] {
            XCTAssertFalse(policy.denies(try address(ip)), "\(ip) must be allowed when allowInternal is set")
        }
    }

    func testDeniesLiteralMatchesResolvedDecision() {
        let policy = EgressPolicy.default
        XCTAssertTrue(policy.deniesLiteral("169.254.169.254"))
        XCTAssertTrue(policy.deniesLiteral("127.0.0.1"))
        XCTAssertTrue(policy.deniesLiteral("::1"))
        XCTAssertFalse(policy.deniesLiteral("8.8.8.8"))
        XCTAssertFalse(policy.deniesLiteral("example.com"), "a hostname is decided after resolution, not by literal check")
    }

    func testLoopbackClassification() throws {
        XCTAssertTrue(EgressPolicy.isLoopback(try address("127.0.0.1")))
        XCTAssertTrue(EgressPolicy.isLoopback(try address("127.5.6.7")))
        XCTAssertTrue(EgressPolicy.isLoopback(try address("::1")))
        XCTAssertFalse(EgressPolicy.isLoopback(try address("10.0.0.1")))
        XCTAssertFalse(EgressPolicy.isLoopback(try address("169.254.169.254")))
    }
}

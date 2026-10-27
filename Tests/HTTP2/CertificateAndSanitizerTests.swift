import X509
import XCTest

@testable import SwiftMITM

final class CertificateAuthorityTests: XCTestCase {
    func testLeafIsSignedByRootWithServerAuthAndSAN() throws {
        let ca = try CertificateAuthority()
        let (leaf, _) = try ca.makeLeafCertificate(forHost: "api.example.com")

        XCTAssertEqual(leaf.issuer, ca.caCertificate.subject)

        let san = try XCTUnwrap(leaf.extensions.subjectAlternativeNames)
        XCTAssertTrue(san.contains(.dnsName("api.example.com")))

        let eku = try XCTUnwrap(leaf.extensions.extendedKeyUsage)
        XCTAssertTrue(eku.contains(.serverAuth))

        let basicConstraints = try XCTUnwrap(leaf.extensions.basicConstraints)
        XCTAssertEqual(basicConstraints, .notCertificateAuthority)
    }

    func testIPv4LeafUsesIPAddressSANOnly() throws {
        let ca = try CertificateAuthority()
        let (leaf, _) = try ca.makeLeafCertificate(forHost: "192.0.2.10")
        let san = try XCTUnwrap(leaf.extensions.subjectAlternativeNames)

        XCTAssertEqual(san.count, 1)
        guard case let .ipAddress(address) = san.first else {
            return XCTFail("IPv4 leaf must use an IP-address SAN")
        }
        XCTAssertEqual(Array(address.bytes), [192, 0, 2, 10])
        XCTAssertFalse(san.contains(.dnsName("192.0.2.10")))
    }

    func testIPv6LeafUsesIPAddressSANOnly() throws {
        let ca = try CertificateAuthority()
        let (leaf, _) = try ca.makeLeafCertificate(forHost: "2001:db8::1")
        let san = try XCTUnwrap(leaf.extensions.subjectAlternativeNames)

        XCTAssertEqual(san.count, 1)
        guard case let .ipAddress(address) = san.first else {
            return XCTFail("IPv6 leaf must use an IP-address SAN")
        }
        XCTAssertEqual(Array(address.bytes), [0x20, 0x01, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        XCTAssertFalse(san.contains(.dnsName("2001:db8::1")))
    }

    func testRootIsACertificateAuthority() throws {
        let ca = try CertificateAuthority()
        let basicConstraints = try XCTUnwrap(ca.caCertificate.extensions.basicConstraints)
        XCTAssertEqual(basicConstraints, .isCertificateAuthority(maxPathLength: 0))
    }
}

import NIOHPACK
import NIOSSL
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
        guard case .ipAddress(let address) = san.first else {
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
        guard case .ipAddress(let address) = san.first else {
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

    func testDefaultRootUsesSwiftMITMSubject() throws {
        let ca = try CertificateAuthority()
        let expectedSubject = try DistinguishedName {
            CommonName("SwiftMITM Root CA")
            OrganizationName("SwiftMITM")
        }
        XCTAssertEqual(ca.caCertificate.subject, expectedSubject)
    }

    func testLeafConvertsToNIOSSLChainAndCaches() throws {
        let ca = try CertificateAuthority()
        let first = try ca.leaf(forHost: "example.com")
        let second = try ca.leaf(forHost: "example.com")
        XCTAssertEqual(first.certificateChain.count, 2)
        XCTAssertEqual(second.certificateChain.count, 2)
    }

    func testCACertificateParsesAsNIOSSLTrustRoot() throws {
        let ca = try CertificateAuthority()
        XCTAssertNoThrow(
            try NIOSSLCertificate(bytes: Array(ca.caCertificatePEM.utf8), format: .pem)
        )
    }
}

final class PseudoHeaderSanitizerTests: XCTestCase {
    private func makeHeaders(_ pairs: [(String, String)]) -> HPACKHeaders {
        var headers = HPACKHeaders()
        for (name, value) in pairs {
            headers.add(name: name, value: value)
        }
        return headers
    }

    func testValidRequestPassesAndStripsPseudoHeaders() {
        let headers = makeHeaders([
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/api/v1"),
            ("accept", "application/json")
        ])
        guard case .success(let line) = PseudoHeaderSanitizer.sanitizeRequest(headers) else {
            return XCTFail("expected sanitization to pass")
        }
        XCTAssertEqual(line.method, "GET")
        XCTAssertEqual(line.authority, "example.com")
        XCTAssertEqual(line.path, "/api/v1")
        XCTAssertEqual(line.headers, [HTTPHeaderField(name: "accept", value: "application/json")])
    }

    func testCRLFInjectionInPathRejected() {
        let headers = makeHeaders([
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/api\r\nX-Injected: evil")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.illegalCharacterInPseudoHeader(":path"))
        )
    }

    func testCRLFInjectionInAuthorityRejected() {
        let headers = makeHeaders([
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.com\r\nHost: evil.com"),
            (":path", "/")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.illegalCharacterInPseudoHeader(":authority"))
        )
    }

    func testTransferEncodingRejected() {
        let headers = makeHeaders([
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/"),
            ("transfer-encoding", "chunked")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.illegalTransferEncoding)
        )
    }

    func testConflictingContentLengthRejected() {
        let headers = makeHeaders([
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/"),
            ("content-length", "10"),
            ("content-length", "20")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.conflictingContentLength)
        )
    }

    func testConnectionHeaderRejected() {
        let headers = makeHeaders([
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/"),
            ("connection", "keep-alive")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.connectionSpecificHeader("connection"))
        )
    }

    func testMissingAuthorityRejected() {
        let headers = makeHeaders([
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.missingPseudoHeader(":authority"))
        )
    }

    func testDuplicateMethodRejected() {
        let headers = makeHeaders([
            (":method", "GET"),
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.duplicatePseudoHeader(":method"))
        )
    }

    func testUppercaseHeaderNameRejected() {
        let headers = makeHeaders([
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/"),
            ("X-Custom", "1")
        ])
        XCTAssertEqual(
            PseudoHeaderSanitizer.sanitizeRequest(headers),
            .failure(.uppercaseHeaderName("X-Custom"))
        )
    }
}

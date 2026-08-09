import Crypto
import Foundation
import SwiftMITM
import X509
import XCTest

final class CertificateAuthorityRestorationTests: XCTestCase {
    func testMalformedPrivateKeyReturnsTypedError() {
        assertRestorationError(.invalidPrivateKey) {
            try CertificateAuthority(privateKeyPEM: "not a private key")
        }
    }

    func testMalformedCertificateReturnsTypedError() {
        let key = P256.Signing.PrivateKey()

        assertRestorationError(.invalidCertificate) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: "not a certificate")
        }
    }

    func testMismatchedPrivateKeyAndCertificateAreRejected() throws {
        let key = P256.Signing.PrivateKey()
        let otherKey = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(subjectKey: otherKey)

        assertRestorationError(.privateKeyMismatch) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testCertificateWithoutCABasicConstraintsIsRejected() throws {
        let key = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(
            subjectKey: key,
            basicConstraints: .notCertificateAuthority
        )

        assertRestorationError(.certificateNotAuthority) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testCertificateMissingCABasicConstraintsIsRejected() throws {
        let key = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(subjectKey: key, basicConstraints: nil)

        assertRestorationError(.certificateNotAuthority) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testCertificateWithoutKeyCertSignUsageIsRejected() throws {
        let key = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(
            subjectKey: key,
            keyUsage: KeyUsage(digitalSignature: true)
        )

        assertRestorationError(.certificateCannotSignCertificates) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testCertificateMissingKeyUsageIsRejected() throws {
        let key = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(subjectKey: key, keyUsage: nil)

        assertRestorationError(.certificateCannotSignCertificates) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testNotYetValidCertificateIsRejected() throws {
        let key = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(
            subjectKey: key,
            notValidBefore: Date().addingTimeInterval(3_600),
            notValidAfter: Date().addingTimeInterval(7_200)
        )

        assertRestorationError(.certificateNotYetValid) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testExpiredCertificateIsRejected() throws {
        let key = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(
            subjectKey: key,
            notValidBefore: Date().addingTimeInterval(-7_200),
            notValidAfter: Date().addingTimeInterval(-3_600)
        )

        assertRestorationError(.certificateExpired) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testCertificateWithInvalidSelfSignatureIsRejected() throws {
        let key = P256.Signing.PrivateKey()
        let certificatePEM = try makeCertificatePEM(subjectKey: key, signingKey: P256.Signing.PrivateKey())

        assertRestorationError(.certificateSignatureInvalid) {
            try CertificateAuthority(privateKeyPEM: key.pemRepresentation, certificatePEM: certificatePEM)
        }
    }

    func testGeneratedAuthorityRestoresSuccessfully() throws {
        let generated = try CertificateAuthority.generate()

        XCTAssertNoThrow(
            try CertificateAuthority(
                privateKeyPEM: generated.privateKeyPEM,
                certificatePEM: generated.certificatePEM
            )
        )
    }

    private func assertRestorationError(
        _ expected: CertificateAuthorityRestorationError,
        operation: () throws -> CertificateAuthority,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertThrowsError(try operation(), file: file, line: line) { error in
            XCTAssertEqual(error as? CertificateAuthorityRestorationError, expected, file: file, line: line)
        }
    }

    private func makeCertificatePEM(
        subjectKey: P256.Signing.PrivateKey,
        signingKey: P256.Signing.PrivateKey? = nil,
        basicConstraints: BasicConstraints? = .isCertificateAuthority(maxPathLength: 0),
        keyUsage: KeyUsage? = KeyUsage(keyCertSign: true),
        notValidBefore: Date = Date().addingTimeInterval(-3_600),
        notValidAfter: Date = Date().addingTimeInterval(3_600)
    ) throws -> String {
        let name = try DistinguishedName { CommonName("Restoration Test CA") }
        let certificate = try Certificate(
            version: .v3,
            serialNumber: .init(),
            publicKey: Certificate.PublicKey(subjectKey.publicKey),
            notValidBefore: notValidBefore,
            notValidAfter: notValidAfter,
            issuer: name,
            subject: name,
            extensions: try Certificate.Extensions {
                if let basicConstraints {
                    Critical(basicConstraints)
                }
                if let keyUsage {
                    Critical(keyUsage)
                }
            },
            issuerPrivateKey: Certificate.PrivateKey(signingKey ?? subjectKey)
        )
        return try certificate.serializeAsPEM().pemString
    }
}

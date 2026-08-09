import Crypto
import NIOSSL
import X509
import XCTest

@testable import SwiftMITM

final class CertificateAuthorityRoundTripTests: XCTestCase {
    func testGenerateExportsReusableMaterial() throws {
        let generated = try CertificateAuthority.generate()

        XCTAssertFalse(generated.privateKeyPEM.isEmpty)
        XCTAssertFalse(generated.certificatePEM.isEmpty)
        XCTAssertNoThrow(try P256.Signing.PrivateKey(pemRepresentation: generated.privateKeyPEM))
        XCTAssertNoThrow(try NIOSSLCertificate(bytes: Array(generated.certificatePEM.utf8), format: .pem))
    }

    func testReinitFromExportedMaterialMintsSameChainingLeaves() throws {
        let generated = try CertificateAuthority.generate()
        let restored = try CertificateAuthority(
            privateKeyPEM: generated.privateKeyPEM,
            certificatePEM: generated.certificatePEM
        )

        XCTAssertEqual(restored.caCertificatePEM, generated.authority.caCertificatePEM)
        XCTAssertEqual(restored.caCertificate, generated.authority.caCertificate)

        let (leaf, _) = try restored.makeLeafCertificate(forHost: "api.example.com")
        XCTAssertEqual(leaf.issuer, generated.authority.caCertificate.subject)

        let minted = try restored.leaf(forHost: "api.example.com")
        XCTAssertEqual(minted.certificateChain.count, 2)
    }

    func testReinitFromKeyOnlyDerivesWorkingRoot() throws {
        let generated = try CertificateAuthority.generate()
        let restored = try CertificateAuthority(privateKeyPEM: generated.privateKeyPEM)

        let basicConstraints = try XCTUnwrap(restored.caCertificate.extensions.basicConstraints)
        XCTAssertEqual(basicConstraints, .isCertificateAuthority(maxPathLength: 0))

        let (leaf, _) = try restored.makeLeafCertificate(forHost: "example.com")
        XCTAssertEqual(leaf.issuer, restored.caCertificate.subject)
    }
}

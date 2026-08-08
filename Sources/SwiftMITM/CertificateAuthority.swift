import Crypto
import Foundation
import NIOConcurrencyHelpers
import NIOSSL
import SwiftASN1
import X509

public struct MintedIdentity: Sendable {
    public let certificateChain: [NIOSSLCertificateSource]
    public let privateKey: NIOSSLPrivateKeySource
}

/// Exported CA material: `privateKeyPEM` is `P256.Signing.PrivateKey.pemRepresentation`, the form Keychain persists.
public struct GeneratedAuthority: Sendable {
    public let authority: CertificateAuthority
    public let privateKeyPEM: String
    public let certificatePEM: String
}

public final class CertificateAuthority: Sendable {
    public static let defaultCommonName = "APIGhost MITM Root"

    public let caCertificate: Certificate
    public let caCertificatePEM: String

    private let caPrivateKey: Certificate.PrivateKey
    private let leafCache: NIOLockedValueBox<[String: MintedIdentity]>

    private init(caKey: P256.Signing.PrivateKey, certificate: Certificate) throws {
        self.caCertificate = certificate
        self.caPrivateKey = Certificate.PrivateKey(caKey)
        self.caCertificatePEM = try certificate.serializeAsPEM().pemString
        self.leafCache = NIOLockedValueBox([:])
    }

    public convenience init(commonName: String = CertificateAuthority.defaultCommonName) throws {
        let caKey = P256.Signing.PrivateKey()
        try self.init(caKey: caKey, certificate: Self.makeRootCertificate(key: caKey, commonName: commonName))
    }

    /// Adopts existing CA material; `privateKeyPEM` is a P256 key in `P256.Signing.PrivateKey.pemRepresentation` form.
    /// A nil `certificatePEM` re-derives the root cert from the key (fresh serial, same key + subject).
    public convenience init(privateKeyPEM: String, certificatePEM: String? = nil) throws {
        let caKey = try P256.Signing.PrivateKey(pemRepresentation: privateKeyPEM)
        let certificate =
            try certificatePEM.map { try Certificate(pemEncoded: $0) }
            ?? Self.makeRootCertificate(key: caKey, commonName: Self.defaultCommonName)
        try self.init(caKey: caKey, certificate: certificate)
    }

    /// Creates a fresh CA and exports its material for persistence (P256 key PEM + cert PEM).
    public static func generate(
        commonName: String = CertificateAuthority.defaultCommonName
    ) throws -> GeneratedAuthority {
        let caKey = P256.Signing.PrivateKey()
        let authority = try CertificateAuthority(
            caKey: caKey,
            certificate: Self.makeRootCertificate(key: caKey, commonName: commonName)
        )
        return GeneratedAuthority(
            authority: authority,
            privateKeyPEM: caKey.pemRepresentation,
            certificatePEM: authority.caCertificatePEM
        )
    }

    private static func makeRootCertificate(
        key caKey: P256.Signing.PrivateKey,
        commonName: String
    ) throws -> Certificate {
        let privateKey = Certificate.PrivateKey(caKey)
        let name = try DistinguishedName {
            CommonName(commonName)
            OrganizationName("APIGhost")
        }
        let now = Date()
        return try Certificate(
            version: .v3,
            serialNumber: Certificate.SerialNumber(),
            publicKey: Certificate.PublicKey(caKey.publicKey),
            notValidBefore: now.addingTimeInterval(-3600),
            notValidAfter: now.addingTimeInterval(60 * 60 * 24 * 3650),
            issuer: name,
            subject: name,
            extensions: try Certificate.Extensions {
                Critical(BasicConstraints.isCertificateAuthority(maxPathLength: 0))
                Critical(KeyUsage(keyCertSign: true, cRLSign: true))
            },
            issuerPrivateKey: privateKey
        )
    }

    /// DER of a freshly minted leaf for `host` — for SecTrust verification only; never persisted, key discarded.
    public func leafCertificateDER(forHost host: String) throws -> Data {
        let (leaf, _) = try makeLeafCertificate(forHost: host.lowercased())
        var serializer = DER.Serializer()
        try serializer.serialize(leaf)
        return Data(serializer.serializedBytes)
    }

    public func leaf(forHost host: String) throws -> MintedIdentity {
        let key = host.lowercased()
        if let cached = leafCache.withLockedValue({ $0[key] }) {
            return cached
        }
        let identity = try mintLeaf(forHost: key)
        leafCache.withLockedValue { $0[key] = identity }
        return identity
    }

    private func mintLeaf(forHost host: String) throws -> MintedIdentity {
        let (leaf, leafPrivate) = try makeLeafCertificate(forHost: host)
        let leafNIO = try Self.nioCertificate(leaf)
        let caNIO = try Self.nioCertificate(caCertificate)
        let keyNIO = try Self.nioPrivateKey(leafPrivate)
        return MintedIdentity(
            certificateChain: [.certificate(leafNIO), .certificate(caNIO)],
            privateKey: .privateKey(keyNIO)
        )
    }

    func makeLeafCertificate(forHost host: String) throws -> (Certificate, Certificate.PrivateKey) {
        let leafKey = P256.Signing.PrivateKey()
        let leafPrivate = Certificate.PrivateKey(leafKey)
        let now = Date()
        let leaf = try Certificate(
            version: .v3,
            serialNumber: Certificate.SerialNumber(),
            publicKey: Certificate.PublicKey(leafKey.publicKey),
            notValidBefore: now.addingTimeInterval(-3600),
            notValidAfter: now.addingTimeInterval(60 * 60 * 24 * 397),
            issuer: caCertificate.subject,
            subject: try DistinguishedName { CommonName(host) },
            extensions: try Certificate.Extensions {
                Critical(BasicConstraints.notCertificateAuthority)
                KeyUsage(digitalSignature: true, keyEncipherment: true)
                try ExtendedKeyUsage([.serverAuth])
                SubjectAlternativeNames([.dnsName(host)])
            },
            issuerPrivateKey: caPrivateKey
        )
        return (leaf, leafPrivate)
    }

    private static func nioCertificate(_ certificate: Certificate) throws -> NIOSSLCertificate {
        let pem = try certificate.serializeAsPEM().pemString
        return try NIOSSLCertificate(bytes: Array(pem.utf8), format: .pem)
    }

    private static func nioPrivateKey(_ key: Certificate.PrivateKey) throws -> NIOSSLPrivateKey {
        let pem = try key.serializeAsPEM().pemString
        return try NIOSSLPrivateKey(bytes: Array(pem.utf8), format: .pem)
    }
}

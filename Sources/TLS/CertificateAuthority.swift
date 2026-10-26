import Crypto
import Darwin
import Foundation
import NIOSSL
import X509

struct MintedIdentity: Sendable {
    let certificateChain: [NIOSSLCertificateSource]
    let privateKey: NIOSSLPrivateKeySource
}

public struct GeneratedAuthority: Sendable {
    public let authority: CertificateAuthority
    public let privateKeyPEM: String
    public let certificatePEM: String
}

public enum CertificateAuthorityRestorationError: Error, Equatable, Sendable {
    case invalidPrivateKey
    case invalidCertificate
    case privateKeyMismatch
    case certificateNotAuthority
    case certificateCannotSignCertificates
    case certificateNotYetValid
    case certificateExpired
    case certificateSignatureInvalid
}

public final class CertificateAuthority: Sendable {
    public static let defaultCommonName = "SwiftMITM Root CA"

    let caCertificate: Certificate
    public let caCertificatePEM: String

    private let caPrivateKey: Certificate.PrivateKey
    private init(caKey: P256.Signing.PrivateKey, certificate: Certificate) throws {
        self.caCertificate = certificate
        self.caPrivateKey = Certificate.PrivateKey(caKey)
        self.caCertificatePEM = try certificate.serializeAsPEM().pemString
    }

    convenience init(commonName: String = CertificateAuthority.defaultCommonName) throws {
        let caKey = P256.Signing.PrivateKey()
        try self.init(caKey: caKey, certificate: Self.makeRootCertificate(key: caKey, commonName: commonName))
    }

    public convenience init(privateKeyPEM: String, certificatePEM: String? = nil) throws {
        let caKey: P256.Signing.PrivateKey
        do {
            caKey = try P256.Signing.PrivateKey(pemRepresentation: privateKeyPEM)
        } catch {
            throw CertificateAuthorityRestorationError.invalidPrivateKey
        }
        guard let certificatePEM else {
            try self.init(
                caKey: caKey,
                certificate: Self.makeRootCertificate(key: caKey, commonName: Self.defaultCommonName)
            )
            return
        }
        let certificate: Certificate
        do {
            certificate = try Certificate(pemEncoded: certificatePEM)
        } catch {
            throw CertificateAuthorityRestorationError.invalidCertificate
        }
        try Self.validateRestoredCertificate(certificate, privateKey: caKey)
        try self.init(caKey: caKey, certificate: certificate)
    }

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
            OrganizationName("SwiftMITM")
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

    private static func validateRestoredCertificate(
        _ certificate: Certificate,
        privateKey: P256.Signing.PrivateKey
    ) throws {
        guard certificate.publicKey == Certificate.PublicKey(privateKey.publicKey) else {
            throw CertificateAuthorityRestorationError.privateKeyMismatch
        }
        let basicConstraints: BasicConstraints?
        let keyUsage: KeyUsage?
        do {
            basicConstraints = try certificate.extensions.basicConstraints
            keyUsage = try certificate.extensions.keyUsage
        } catch {
            throw CertificateAuthorityRestorationError.invalidCertificate
        }
        guard case .isCertificateAuthority = basicConstraints else {
            throw CertificateAuthorityRestorationError.certificateNotAuthority
        }
        guard keyUsage?.keyCertSign == true else {
            throw CertificateAuthorityRestorationError.certificateCannotSignCertificates
        }
        let now = Date()
        guard certificate.notValidBefore <= now else {
            throw CertificateAuthorityRestorationError.certificateNotYetValid
        }
        guard certificate.notValidAfter >= now else {
            throw CertificateAuthorityRestorationError.certificateExpired
        }
        guard certificate.publicKey.isValidSignature(certificate.signature, for: certificate) else {
            throw CertificateAuthorityRestorationError.certificateSignatureInvalid
        }
    }

    func leaf(forHost host: String) throws -> MintedIdentity {
        try mintIdentity(forHost: host.lowercased())
    }

    func mintIdentity(forHost host: String) throws -> MintedIdentity {
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
                SubjectAlternativeNames([Self.subjectAlternativeName(forHost: host)])
            },
            issuerPrivateKey: caPrivateKey
        )
        return (leaf, leafPrivate)
    }

    private static func subjectAlternativeName(forHost host: String) -> GeneralName {
        if let bytes = ipAddressBytes(host) {
            return .ipAddress(.init(contentBytes: bytes[...]))
        }
        return .dnsName(host)
    }

    private static func ipAddressBytes(_ host: String) -> [UInt8]? {
        var ipv4 = in_addr()
        if host.withCString({ inet_pton(AF_INET, $0, &ipv4) }) == 1 {
            return withUnsafeBytes(of: &ipv4) { Array($0) }
        }
        var ipv6 = in6_addr()
        if host.withCString({ inet_pton(AF_INET6, $0, &ipv6) }) == 1 {
            return withUnsafeBytes(of: &ipv6) { Array($0) }
        }
        return nil
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

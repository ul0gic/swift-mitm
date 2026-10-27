import CryptoKit
import Foundation
import NIOSSL
import Security

func release022ReportRunnerTrust(certificate: URL) throws {
    let pem = try String(contentsOf: certificate, encoding: .utf8)
    let der = Data(try NIOSSLCertificate(bytes: Array(pem.utf8), format: .pem).toDERBytes())
    guard let root = SecCertificateCreateWithData(nil, der as CFData) else {
        throw Release022ClientError.commandFailed("runner trust root could not be decoded")
    }
    var settings: CFArray?
    let settingsStatus = SecTrustSettingsCopyTrustSettings(root, .admin, &settings)
    var certificates: CFArray?
    let membershipStatus = SecTrustSettingsCopyCertificates(.admin, &certificates)
    let rootPresent = (certificates as? [SecCertificate])?.contains {
        SecCertificateCopyData($0) as Data == der
    }
    let fingerprint = SHA256.hash(data: der).map { String(format: "%02x", $0) }.joined()
    let facts = "fingerprint=\(fingerprint), settingsStatus=\(settingsStatus), "
        + "membershipStatus=\(membershipStatus), rootPresent=\(String(describing: rootPresent))"
    FileHandle.standardError.write(Data("Chromium admin trust: \(facts)\n".utf8))
    let entries = settings as? [NSDictionary]
    if let entries {
        release022ReportTrustPolicies(entries)
    }
    guard settingsStatus == errSecSuccess, membershipStatus == errSecSuccess,
          rootPresent == true, entries != nil else {
        throw Release022ClientError.commandFailed("runner admin trust is not visible to the browser user")
    }
}

private func release022ReportTrustPolicies(_ entries: [NSDictionary]) {
    let sslPolicy = SecPolicyCreateSSL(true, nil)
    FileHandle.standardError.write(Data("Chromium admin trust: policyCount=\(entries.count)\n".utf8))
    for (index, entry) in entries.prefix(4).enumerated() {
        let policy = entry[kSecTrustSettingsPolicy]
        let matchesSSL = policy.map { CFEqual($0 as CFTypeRef, sslPolicy) }
        let result = (entry[kSecTrustSettingsResult] as? NSNumber)?.intValue
        let facts = "policy=\(index), policyPresent=\(policy != nil), "
            + "sslPolicyEqual=\(String(describing: matchesSSL)), result=\(String(describing: result)), "
            + "applicationScoped=\(entry[kSecTrustSettingsApplication] != nil), "
            + "policyStringScoped=\(entry[kSecTrustSettingsPolicyString] != nil)"
        FileHandle.standardError.write(Data("Chromium admin trust: \(facts)\n".utf8))
    }
}

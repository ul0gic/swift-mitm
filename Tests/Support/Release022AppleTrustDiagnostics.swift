import Foundation
import Security

func release022TrustDiagnostic(trust: SecTrust, expectedRoot: SecCertificate, failure: CFError?) -> String {
    let chainFacts: String
    if let chain = SecTrustCopyCertificateChain(trust) as? [SecCertificate] {
        let expectedDER = SecCertificateCopyData(expectedRoot) as Data
        let rootPresent = chain.contains { SecCertificateCopyData($0) as Data == expectedDER }
        chainFacts = "chainCount=\(chain.count), expectedRootPresent=\(rootPresent)"
    } else {
        chainFacts = "certificateChainUnavailable"
    }
    guard let failure else { return chainFacts + "; no evaluation error supplied" }
    let description = String(String(describing: CFErrorCopyDescription(failure)).prefix(512))
    let reason = String(String(describing: CFErrorCopyFailureReason(failure)).prefix(512))
    return chainFacts + "; domain=\(String(describing: CFErrorGetDomain(failure))), code=\(CFErrorGetCode(failure)); "
        + "description=\(description); reason=\(reason)"
}

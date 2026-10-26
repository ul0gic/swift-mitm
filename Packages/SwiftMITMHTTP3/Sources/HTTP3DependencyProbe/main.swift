import NIOHTTP3
import NIOQUIC
import SwiftMITM

let generated = try CertificateAuthority.generate(commonName: "SwiftMITM HTTP3 dependency probe")
_ = try CertificateAuthority(
    privateKeyPEM: generated.privateKeyPEM,
    certificatePEM: generated.certificatePEM
)
print("Combined graph linked; core authority generation and restoration completed.")

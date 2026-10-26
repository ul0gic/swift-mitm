import Foundation
import NIOHTTP3
import NIOQUIC
import SwiftMITM

let generated = try CertificateAuthority.generate(commonName: "SwiftMITM HTTP3 dependency probe")
_ = try CertificateAuthority(
    privateKeyPEM: generated.privateKeyPEM,
    certificatePEM: generated.certificatePEM
)
try FileHandle.standardOutput.write(contentsOf: Data(
    "Combined graph linked; core authority generation and restoration completed.\n".utf8
))

# Certificate Authority and Trust

Keep certificate creation, persistence, trust installation, and TLS interception as separate responsibilities.

## Overview

### Generate once and retain both PEM values

``CertificateAuthority/generate(commonName:)`` returns a ``GeneratedAuthority`` containing the live authority, its P-256 private key PEM, and its self-signed root certificate PEM.

Persist the key and certificate as one identity. Protect the private key as a credential: do not log it, put it in source control, store it in defaults, or write it to an unprotected file. SwiftMITM does not choose a persistence mechanism and does not interact with Keychain.

The embedding application owns the user-facing trust workflow. It must explain the interception scope, obtain authorization, install the root into the appropriate client or system trust context, and remove that trust when the feature is no longer authorized.

### Restore at the public boundary

``CertificateAuthority/init(privateKeyPEM:certificatePEM:)`` validates supplied material before constructing a live authority.

| Error | Meaning |
| --- | --- |
| ``CertificateAuthorityRestorationError/invalidPrivateKey`` | The private-key PEM is malformed or is not a supported P-256 signing key. |
| ``CertificateAuthorityRestorationError/invalidCertificate`` | The certificate PEM or its relevant extensions cannot be parsed. |
| ``CertificateAuthorityRestorationError/privateKeyMismatch`` | The certificate public key does not match the supplied private key. |
| ``CertificateAuthorityRestorationError/certificateNotAuthority`` | CA Basic Constraints are absent or do not authorize CA use. |
| ``CertificateAuthorityRestorationError/certificateCannotSignCertificates`` | Key Usage is absent or does not include certificate signing. |
| ``CertificateAuthorityRestorationError/certificateNotYetValid`` | The certificate validity interval has not begun. |
| ``CertificateAuthorityRestorationError/certificateExpired`` | The certificate validity interval has ended. |
| ``CertificateAuthorityRestorationError/certificateSignatureInvalid`` | The certificate's self-signature is invalid. |

Passing only `privateKeyPEM` creates a new self-signed root with the default subject. That path can produce a working authority but is not a replacement for restoring the exact certificate already installed by users. Persist and restore `certificatePEM` whenever trust must survive an application restart.

### Understand issued host identities

SwiftMITM issues a 397-day leaf identity for each intercepted target. DNS hosts receive DNS SANs; IPv4 and IPv6 literals receive IP-address SANs. Explicit `CONNECT` uses its authority as the fallback identity when the client omits SNI. Trusted transparent ingress uses the PROXY v2 destination unless TLS SNI supplies a DNS identity; SNI never changes the physical upstream route.

Each running proxy keeps at most 256 completed leaf identities and 256 distinct pending mints. Requests for the same host share one in-flight mint, and cache-miss cryptography runs outside SwiftNIO event loops. Stopping the proxy releases this per-run TLS state.

### Keep upstream and downstream trust distinct

The generated CA establishes trust between the authorized client and SwiftMITM. ``ProxyServer/UpstreamPolicy`` separately controls how SwiftMITM authenticates origins.

Upstream verification is enabled by default. `additionalTrustRootsPEM` augments the system roots for private or test origins; it does not replace them. Setting `verifyCertificate` to `false` disables origin identity protection and should be limited to an explicitly controlled environment.

Certificate pinning remains effective. SwiftMITM does not bypass a client that requires a specific origin certificate or public key.

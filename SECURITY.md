# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| `0.1.x` | Yes |
| Earlier versions | No |

The latest tagged minor line and `main` receive security fixes.

## Reporting a vulnerability

Report vulnerabilities through [GitHub private vulnerability reporting](https://github.com/ul0gic/swift-mitm/security/advisories/new). Do not include exploit details, private keys, credentials, captured payloads, or other sensitive material in a public issue.

If private reporting is unavailable, open a public issue containing no vulnerability details and request a private contact channel. A maintainer will coordinate validation, remediation, and disclosure timing with the reporter.

Security reports should identify the affected revision and describe the impact, prerequisites, and a minimal non-sensitive reproduction. High-value reports include trust bypasses, egress-policy bypasses, remotely triggerable crashes or unbounded allocation, private-key exposure, and shutdown behavior that leaves interception channels active.

## Scope

SwiftMITM is an authorized interception engine. Certificate installation, CA-key storage, captured-data handling, client proxy configuration, listener exposure after non-loopback opt-in, and use authorization are consumer responsibilities unless the defect is caused by SwiftMITM violating its documented contract.

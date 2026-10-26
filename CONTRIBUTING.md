# Contributing

Open an issue before proposing a behavior or public-API change. Keep changes scoped to the standalone SwiftPM library; application UI, persistence, and host-product behavior do not belong in SwiftMITM.

## Verification

GitHub Actions owns verification; no local gate is required. Pull requests receive tests, CodeQL analysis, and dependency review before merging. Markdown-only changes skip the Swift test build. CodeQL also scans weekly, and Dependabot monitors dependency advisories and proposes updates.

Keep `Package.resolved` current when changing dependencies so review covers the versions the package actually builds. New behavior needs focused consumer-facing evidence and documentation for changed lifecycle or security contracts.

Do not add production dependencies without prior approval. Do not include private keys, credentials, captured payloads, or product-specific identifiers in code, fixtures, logs, issues, or pull requests.

By contributing, you agree that your contribution is licensed under the repository's [MIT License](LICENSE).

# Contributing

Open an issue before proposing a behavior or public-API change. Keep changes scoped to the standalone SwiftPM library; application UI, persistence, and host-product behavior do not belong in SwiftMITM.

## Verification

GitHub Actions builds the package, builds the FirstCapture consumer, runs a proxy smoke check, and reviews dependency changes. Dependabot monitors dependency advisories and proposes updates. Run SwiftFormat and SwiftLint locally before pushing.

Keep `Package.resolved` current when changing dependencies so review covers the versions the package actually builds. New behavior needs focused consumer-facing evidence and documentation for changed lifecycle or security contracts.

Do not add production dependencies without prior approval. Do not include private keys, credentials, captured payloads, or product-specific identifiers in code, fixtures, logs, issues, or pull requests.

By contributing, you agree that your contribution is licensed under the repository's [MIT License](LICENSE).

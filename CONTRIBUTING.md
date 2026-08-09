# Contributing

Open an issue before proposing a behavior or public-API change. Keep changes scoped to the standalone SwiftPM library; application UI, persistence, and host-product behavior do not belong in SwiftMITM.

## Local gate

Install Swift 6, SwiftFormat, and SwiftLint, then run:

```sh
Scripts/verify.sh
```

The gate checks formatting, strict lint, non-load tests in parallel, resource-intensive tests serially in an isolated process, a release build, and DocC warnings. The partitions are exhaustive and run every test exactly once without allowing streaming, memory, or production-backpressure measurements to starve unrelated integration tests. New behavior requires focused consumer-facing tests, bounded hostile-input behavior where applicable, and documentation for public lifecycle or security contracts.

Do not add production dependencies without prior approval. Do not include private keys, credentials, captured payloads, or product-specific identifiers in code, fixtures, logs, issues, or pull requests.

By contributing, you agree that your contribution is licensed under the repository's [MIT License](LICENSE).

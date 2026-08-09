# Contributing

Open an issue before proposing a behavior or public-API change. Keep changes scoped to the standalone SwiftPM library; application UI, persistence, and host-product behavior do not belong in SwiftMITM.

## Local gate

Install Swift 6, SwiftFormat, and SwiftLint, then run:

```sh
Scripts/verify.sh
```

The gate checks formatting, strict lint, all tests serially, non-load tests again in parallel, a release build, and DocC warnings. Resource-intensive streaming, memory, and production-backpressure tests remain in the complete serial run and are intentionally excluded from the parallel replay. New behavior requires focused consumer-facing tests, bounded hostile-input behavior where applicable, and documentation for public lifecycle or security contracts.

Do not add production dependencies without prior approval. Do not include private keys, credentials, captured payloads, or product-specific identifiers in code, fixtures, logs, issues, or pull requests.

By contributing, you agree that your contribution is licensed under the repository's [MIT License](LICENSE).

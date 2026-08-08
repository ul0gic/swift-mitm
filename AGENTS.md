# SwiftMITM Agent Guide

SwiftMITM is a standalone SwiftPM library for authorized TLS interception. Keep the package independent of API Ghost or any other host application: no application UI, persistence, product state, or host-specific branding belongs here.

## Required context

- For every task that reads, reviews, writes, or changes Swift source, tests, or `Package.swift`, invoke `$swiftmitm-engineering-standards` before acting and follow every required reference.
- Before changing behavior, read `README.md`, `Package.swift`, the relevant source and tests, `.project/build-plan.md`, and related open and closed issues.
- Treat `Package.swift` and executable tests as authoritative when documentation disagrees with implementation.
- Do not add or change production dependencies without an explicit, demonstrated need and lead approval.

## Engineering expectations

- Make the smallest complete change; preserve unrelated work and avoid speculative abstractions.
- Source comments default to zero. An indispensable comment must occupy exactly one physical line; this overrides all other documentation conventions.
- Keep public APIs intentional, minimal, `Sendable` where required, and covered by consumer-facing tests.
- Preserve SwiftNIO event-loop confinement, streaming, backpressure, bounded allocation, and deterministic shutdown behavior.
- Preserve secure defaults: loopback binding, upstream TLS verification, restricted internal egress, bounded capture, and no logging of bodies, credentials, or private keys.
- Never introduce API Ghost or legacy-tool identifiers into active source, tests, logs, certificate metadata, or public documentation.

## Commands

Required local tools: Swift 6, SwiftFormat, SwiftLint, and `jq`.

```sh
swift build
swift build -c release
swift test
swift test --filter TestCaseName/testName

swiftformat Sources Tests Package.swift --config .swiftformat
swiftformat Sources Tests Package.swift --lint --config .swiftformat
swiftlint lint --strict --quiet --config .swiftlint.yml
```

- Run focused tests while iterating, then run the complete applicable gate before finishing.
- `.codex/hooks/format.sh` formats Swift files changed through `apply_patch`.
- `.codex/hooks/verify.sh` is the Stop-time contract: SwiftFormat lint, strict SwiftLint, then `swift test`.
- Treat `.codex/hooks.json`, `.swiftformat`, and `.swiftlint.yml` as executable policy; do not bypass or weaken them to make a change pass.

## Project structure

| Path | Purpose |
| --- | --- |
| `Sources/SwiftMITM/` | Library implementation and supported public surface |
| `Tests/SwiftMITMTests/` | Unit, integration, TLS, protocol-hardening, and streaming tests |
| `Package.swift` | Package products, platforms, targets, and direct dependencies |
| `README.md` | Consumer-facing setup, integration, security, and support contract |
| `.agents/skills/` | Repo-scoped engineering standards loaded by Codex |
| `.codex/agents/` | Project-scoped custom agent TOML definitions |
| `.codex/hooks.json`, `.codex/hooks/` | Automatic formatting and final verification |
| `.project/prd.md` | Product scope and release requirements |
| `.project/tech-stack.md` | Implemented architecture and technical constraints |
| `.project/build-plan.md` | Current phases, progress, gates, and ownership |
| `.project/changelog.md` | User-visible package changes and release history |
| `.project/issues/` | Open and closed findings with durable evidence |

## Using agents

| Agent | Use for |
| --- | --- |
| `swift-engineer` | Swift implementation, SwiftNIO/TLS architecture, concurrency, performance, and public API work |
| `qa-engineer` | Test strategy, adversarial cases, integration coverage, flake analysis, and quality gates |
| `documentation-engineer` | README, DocC, changelog, API guidance, and documentation verification |
| `devops-engineer` | CI, release automation, dependency automation, and reproducible build infrastructure |

- Delegate only bounded work with clear ownership; use parallel agents when tasks are genuinely independent.
- Never assign multiple writers to the same file or conflict zone. The main agent integrates cross-cutting work and owns `.project/` records.
- Every agent follows this file, invokes the mandatory SwiftMITM skill for Swift work, runs focused verification, and files out-of-scope findings.

## Issue filing

- Filing is mandatory for every material out-of-scope defect, security concern, performance risk, missing test, or technical-debt finding.
- Search both `.project/issues/open/` and `.project/issues/closed/` before filing; update an existing issue instead of duplicating it.
- Use `.project/issues/ISSUE_TEMPLATE.md`, choose the next unused prefix number, and create the file under `.project/issues/open/`.
- Record confirmed evidence, impact, affected paths, and verifiable acceptance criteria. Never include secrets, private keys, credentials, or captured payloads.
- Do not expand the active task to fix the finding unless the user adds it to scope.
- Move an issue to `closed/` only after the resolution and verification are recorded.
- Modify other `.project/` records only when the task explicitly includes project administration or milestone tracking.

## Completion gate

- Relevant focused tests pass.
- SwiftFormat lint and strict SwiftLint pass with zero SwiftMITM-owned violations.
- The full `swift test` suite passes for implementation changes.
- Public behavior, security responsibilities, and release-visible changes are reflected in README, project docs, or changelog when applicable.
- Newly discovered out-of-scope work has a filed issue.

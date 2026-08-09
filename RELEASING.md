# Releasing SwiftMITM

SwiftMITM releases use stable Semantic Versioning tags without a `v` prefix. Pushing a valid tag runs the same macOS release gate as pull requests and creates a GitHub release from `RELEASE_NOTES.md` only after verification succeeds.

## Repository setup

Before the first release:

- Configure `https://github.com/ul0gic/swift-mitm` as the canonical remote.
- Require the `CI / Verify` check on `main` and prevent direct release changes that bypass review.
- Enable GitHub private vulnerability reporting, Dependabot alerts, and Dependabot security updates.
- Confirm GitHub Actions has permission to create repository releases.

## Candidate gate

- Update `RELEASE_NOTES.md` so its first heading exactly matches `# SwiftMITM <version>`.
- Move the matching changelog content from `Unreleased` to a dated version section.
- Confirm no open issue is an unacknowledged release blocker.
- Run `Scripts/validate-release-tag.sh <version>`.
- Run `Scripts/verify.sh` from a clean checkout.
- Confirm the external consumer integration resolves the intended tag rather than `main`.
- Obtain explicit lead approval for the public API and release tag.

## Tag and verify

Create and push an annotated tag only after `main` CI is green:

```sh
git tag -a 0.1.0 -m "SwiftMITM 0.1.0"
git push origin 0.1.0
```

The release workflow rejects malformed tags or mismatched release notes, reruns the complete gate on macOS, and then creates the GitHub release. Verify the release assets and resolve SwiftMITM from the tag in a fresh external package before announcing it.

Do not retag a published version. Correct a release defect with a new patch version.

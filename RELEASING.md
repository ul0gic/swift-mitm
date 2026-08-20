# Releasing SwiftMITM

SwiftMITM releases use stable Semantic Versioning tags without a `v` prefix. Pushing a valid tag creates a GitHub release from `RELEASE_NOTES.md` only when the tag targets a commit on `main` whose required CI run succeeded.

## Release prerequisites

Before every release, confirm that:

- `https://github.com/ul0gic/swift-mitm` is the canonical remote.
- `CI / Verify` is required on `main`, and direct release changes cannot bypass review.
- GitHub private vulnerability reporting, Dependabot alerts, and Dependabot security updates are enabled.
- GitHub Actions can create repository releases.

## Candidate gate

- Set `RELEASE_VERSION` to the intended stable Semantic Version release tag.
- Update `RELEASE_NOTES.md` so its first heading exactly matches `# SwiftMITM $RELEASE_VERSION`.
- Move the matching changelog content from `Unreleased` to a dated version section.
- Confirm no open issue is an unacknowledged release blocker.
- Run `Scripts/validate-release-tag.sh "$RELEASE_VERSION"`.
- Run `RELEASE_VERSION="$RELEASE_VERSION" Scripts/verify.sh` from a clean checkout.
- Confirm the external consumer integration resolves `"$RELEASE_VERSION"` rather than `main`.
- Obtain explicit lead approval for the public API and release tag.

## Tag and verify

Create and push an annotated tag only after `main` CI is green:

```sh
git tag -a "$RELEASE_VERSION" -m "SwiftMITM $RELEASE_VERSION"
git push origin "$RELEASE_VERSION"
```

The release workflow rejects malformed or lightweight tags, mismatched release notes, commits outside `main`, and commits without a successful `main` CI run. It does not repeat the complete build on a clean tag runner because the immutable commit has already passed the protected gate. If publication fails after verification for an infrastructure reason, rerun it without moving the tag:

```sh
gh workflow run release.yml -f tag="$RELEASE_VERSION"
```

Verify the release assets and resolve SwiftMITM from the tag in a fresh external package before announcing it.

Do not retag a published version. Correct a release defect with a new patch version.

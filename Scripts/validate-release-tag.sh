#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${1:-}"
release_version="${RELEASE_VERSION:-}"
stable_semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'

if [[ ! "$tag" =~ $stable_semver_pattern ]]; then
    echo "release tag must be a stable Semantic Version in MAJOR.MINOR.PATCH form" >&2
    exit 1
fi

if [[ -z "$release_version" ]]; then
    echo "RELEASE_VERSION is required for release-candidate validation" >&2
    exit 1
fi

if [[ ! "$release_version" =~ $stable_semver_pattern ]]; then
    echo "RELEASE_VERSION must be a stable Semantic Version in MAJOR.MINOR.PATCH form" >&2
    exit 1
fi

if [[ "$tag" != "$release_version" ]]; then
    echo "release tag $tag does not match RELEASE_VERSION $release_version" >&2
    exit 1
fi

if [[ "$(head -n 1 "$repository_root/RELEASE_NOTES.md")" != "# SwiftMITM $tag" ]]; then
    echo "RELEASE_NOTES.md must begin with release heading: # SwiftMITM $tag" >&2
    exit 1
fi

if ! grep -Fq "## $tag - " "$repository_root/CHANGELOG.md"; then
    echo "CHANGELOG.md must contain a dated section for $tag" >&2
    exit 1
fi

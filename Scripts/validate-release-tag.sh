#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${1:-}"

if [[ ! "$tag" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    echo "release tag must be a stable Semantic Version such as 0.1.0" >&2
    exit 1
fi

if ! grep -Fqx "# SwiftMITM $tag" "$repository_root/RELEASE_NOTES.md"; then
    echo "RELEASE_NOTES.md must begin with release heading: # SwiftMITM $tag" >&2
    exit 1
fi

if ! grep -Fq "## $tag - " "$repository_root/CHANGELOG.md"; then
    echo "CHANGELOG.md must contain a dated section for $tag" >&2
    exit 1
fi

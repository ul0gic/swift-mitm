#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
documentation_output="$(mktemp -d "${TMPDIR:-/tmp}/swiftmitm-docc.XXXXXX")"
release_version="${RELEASE_VERSION:-0.0.0-dev}"
semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(\+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$'

if [[ ! "$release_version" =~ $semver_pattern ]]; then
    echo "RELEASE_VERSION must be a valid Semantic Version" >&2
    exit 1
fi

cleanup() {
    rm -rf -- "$documentation_output"
}

trap cleanup EXIT

cd "$repository_root"
swift build --build-tests
swift package dump-symbol-graph --minimum-access-level public
symbol_graph_directory="$(find .build -type d -name symbolgraph -print -quit)"
test -n "$symbol_graph_directory"
xcrun docc convert Sources/SwiftMITM/SwiftMITM.docc \
    --additional-symbol-graph-dir "$symbol_graph_directory" \
    --output-path "$documentation_output/SwiftMITM.doccarchive" \
    --fallback-display-name SwiftMITM \
    --fallback-bundle-identifier com.ul0gic.SwiftMITM \
    --fallback-bundle-version "$release_version" \
    --warnings-as-errors

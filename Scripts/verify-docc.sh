#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
documentation_output="$(mktemp -d "${TMPDIR:-/tmp}/swiftmitm-docc.XXXXXX")"

cleanup() {
    rm -rf -- "$documentation_output"
}

trap cleanup EXIT

cd "$repository_root"
swift package dump-symbol-graph --minimum-access-level public
symbol_graph_directory="$(find .build -type d -name symbolgraph -print -quit)"
test -n "$symbol_graph_directory"
xcrun docc convert Sources/SwiftMITM/SwiftMITM.docc \
    --additional-symbol-graph-dir "$symbol_graph_directory" \
    --output-path "$documentation_output/SwiftMITM.doccarchive" \
    --fallback-display-name SwiftMITM \
    --fallback-bundle-identifier com.ul0gic.SwiftMITM \
    --fallback-bundle-version 0.1.0 \
    --warnings-as-errors

#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repository_root"
"$repository_root/Scripts/verify-action-pinning.sh"
swiftformat Sources Tests Package.swift --lint --config .swiftformat
swiftlint lint --strict --quiet --config .swiftlint.yml
swift build
swift build -c release
swift test
swift test --parallel
"$repository_root/Scripts/verify-docc.sh"

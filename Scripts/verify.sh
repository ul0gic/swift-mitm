#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repository_root"

verify_quality() {
    swiftformat Sources Tests Package.swift --lint --config .swiftformat
    swiftlint lint --strict --quiet --config .swiftlint.yml
}

verify_tests() {
    swift test
    swift test --skip-build --parallel \
        --skip 'SwiftMITMTests\.(ProductionProxyBackpressureTests|ProxyDiagnosticTests|StreamingBackpressureTests)'
}

verify_distribution() {
    swift build -c release
    "$repository_root/Scripts/verify-docc.sh"
}

case "${1:-all}" in
quality)
    verify_quality
    ;;
tests)
    verify_tests
    ;;
distribution)
    verify_distribution
    ;;
all)
    "$repository_root/Scripts/verify-action-pinning.sh"
    verify_quality
    verify_tests
    verify_distribution
    ;;
*)
    echo "usage: Scripts/verify.sh [all|quality|tests|distribution]" >&2
    exit 2
    ;;
esac

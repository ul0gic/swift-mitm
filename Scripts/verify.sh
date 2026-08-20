#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repository_root"

verify_quality() {
    swiftformat Sources Tests Package.swift --lint --config .swiftformat
    swiftlint lint --strict --quiet --config .swiftlint.yml
}

verify_tests() {
    local isolated_test_filter='SwiftMITMTests\.(Phase4PublicTransparentTLSTests|Phase6OpaqueResourceTests|Phase6TransparentResourceTests|ProductionProxyBackpressureTests|ProxyDiagnosticTests|PublicProxyHTTP2WebSocketBackpressureTests|PublicProxyHTTP2WebSocketTests|StreamingBackpressureTests)'

    swift test --parallel \
        --skip "$isolated_test_filter"
    swift test --skip-build \
        --filter "$isolated_test_filter"
}

verify_distribution() {
    local release_version="${RELEASE_VERSION:-0.0.0-dev}"

    swift build -c release
    RELEASE_VERSION="$release_version" "$repository_root/Scripts/verify-docc.sh"
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

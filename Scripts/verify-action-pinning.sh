#!/bin/bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
failure=0

while IFS= read -r record; do
    location="${record%%:*}"
    remainder="${record#*:}"
    line_number="${remainder%%:*}"
    declaration="${remainder#*:}"
    reference="${declaration#*uses:}"
    reference="${reference#"${reference%%[![:space:]]*}"}"
    reference="${reference%%#*}"
    reference="${reference%"${reference##*[![:space:]]}"}"

    if [[ "$reference" == ./* ]]; then
        continue
    fi

    revision="${reference##*@}"
    if [[ ! "$revision" =~ ^[0-9a-f]{40}$ ]]; then
        echo "$location:$line_number: external action must be pinned to a full commit SHA: $reference" >&2
        failure=1
    fi
done < <(grep -R -HnE '^[[:space:]]*uses:' "$repository_root/.github/workflows")

exit "$failure"

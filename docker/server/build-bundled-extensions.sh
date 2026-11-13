#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Build VEB files for bundled VillageSQL extensions.
#
# Usage: include_bundled_extensions.sh <dst_dir> <veb_src_dir>
#
# <veb_src_dir>: Directory where built .veb files were placed.
#
# The extension list is read from villagesql/dev_server/bundled_extensions.txt,
# located relative to this script's source tree.

set -euo pipefail

EXTENSIONS_LIST="source/villagesql/dev_server/bundled_extensions.txt"

function log_info() { echo "$*"; }

if [[ ! -f "$EXTENSIONS_LIST" ]]; then
    log_info "Extensions list not found: $EXTENSIONS_LIST"
    exit 1
fi

# Read and parse the list of extensions
while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue

    # Parse "url [branch-or-tag] [key=value ...]" — all fields after url are optional
    read -ra FIELDS <<< "$line"
    SOURCE="${FIELDS[0]%/}"
    BRANCH="${FIELDS[1]:-}"
    REPO_NAME="${SOURCE##*/}"

    # Confirm the extension is bundled
    BUNDLE=true
    for FIELD in "${FIELDS[@]:2}"; do
        [[ "$FIELD" == "bundle=false" || "$FIELD" == "bundle=no" ]] && BUNDLE=false
    done
    if [[ "$BUNDLE" == "false" ]]; then
        log_info "Skipping $REPO_NAME (bundle=false)"
        continue
    fi

    build-bundled-extension.sh "$REPO_NAME"

done < "$EXTENSIONS_LIST"

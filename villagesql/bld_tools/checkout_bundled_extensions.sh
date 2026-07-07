#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Checkout the sources for bundled VillageSQL extensions.
#
# Usage: checkout_bundled_extensions.sh <ext_dir> [extension]
#
# <ext_dir>:        Path for checked out extensions.  Should exist before calling.
# [extension]:      Optional repo name to build only one extension (e.g. vsql-ai).
#                   Omit to build all extensions in bundled_extensions.txt.
#
# The extension list is read from villagesql/dev_server/bundled_extensions.txt,
# located relative to this script's source tree.

set -euo pipefail

EXTENSION_CLONES_DIR="${1:?Usage: $0 <ext_dir> [<ext_filter>]}"
EXTENSION_FILTER="${2:-}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
EXTENSIONS_LIST="$SOURCE_DIR/villagesql/dev_server/bundled_extensions.txt"

if [[ ! -f "$EXTENSIONS_LIST" ]]; then
    die "Extensions list not found: $EXTENSIONS_LIST"
fi

if [[ ! -d "$EXTENSION_CLONES_DIR" ]]; then
    die "Extension directory not found: $EXTENSION_CLONES_DIR (mkdir first)"
fi

while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue

    # Parse "url [branch-or-tag] [key=value ...]" — all fields after url are optional
    read -ra FIELDS <<< "$line"
    SOURCE="${FIELDS[0]%/}"
    BRANCH="${FIELDS[1]:-}"
    REPO_NAME="${SOURCE##*/}"

    if [[ -n "$EXTENSION_FILTER" && "$REPO_NAME" != "$EXTENSION_FILTER" ]]; then
        continue
    fi

    BUNDLE=true
    for FIELD in "${FIELDS[@]:2}"; do
        [[ "$FIELD" == "bundle=false" || "$FIELD" == "bundle=no" ]] && BUNDLE=false
    done
    if [[ "$BUNDLE" == "false" ]]; then
        log_info "Skipping $REPO_NAME (bundle=false)"
        continue
    fi

    log_step "Cloning $REPO_NAME ($SOURCE${BRANCH:+ @ $BRANCH})"

    CLONE_DIR="$EXTENSION_CLONES_DIR/$REPO_NAME"

    CLONE_ARGS=(--depth=1)
    [[ -n "$BRANCH" ]] && CLONE_ARGS+=(--branch "$BRANCH")

    # git clone accepts both remote URLs and local filesystem paths
    if ! git clone "${CLONE_ARGS[@]}" "$SOURCE" "$CLONE_DIR" 2>&1; then
        log_error "Failed to clone $SOURCE"
        FAILED=$((FAILED + 1))
        continue
    fi
done < "$EXTENSIONS_LIST"

#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Checkout the sources for bundled VillageSQL extensions.
#
# Usage: checkout_bundled_extensions.sh <ext_dir> [extension] [include_unbundled]
#
# <ext_dir>:        Path for checked out extensions.  Should exist before calling.
# [extension]:      Optional repo name to build only one extension (e.g. vsql-ai).
#                   Omit to build all extensions in bundled_extensions.txt.
# [include_unbundled]: 0/no (default) to skip bundle=false extensions, 1/yes to
#                   clone them too. They ship with no dev server, but are still
#                   built and tested (e.g. by the sanitizer workflow).
#
# The extension list is read from villagesql/dev_server/bundled_extensions.txt,
# located relative to this script's source tree.

set -euo pipefail

EXTENSION_CLONES_DIR="${1:?Usage: $0 <ext_dir> [<ext_filter>] [include_unbundled]}"
EXTENSION_FILTER="${2:-}"
INCLUDE_UNBUNDLED="${3:-no}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
EXTENSIONS_LIST="$SOURCE_DIR/villagesql/dev_server/bundled_extensions.txt"

case "$INCLUDE_UNBUNDLED" in
    1|yes) INCLUDE_UNBUNDLED=yes ;;
    0|no|"") INCLUDE_UNBUNDLED=no ;;
    *) die "Invalid include_unbundled: $INCLUDE_UNBUNDLED (expected 0/no or 1/yes)" ;;
esac

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

    if [[ "$INCLUDE_UNBUNDLED" == "no" ]]; then
        BUNDLE=true
        for FIELD in "${FIELDS[@]:2}"; do
            [[ "$FIELD" == "bundle=false" || "$FIELD" == "bundle=no" ]] && BUNDLE=false
        done
        if [[ "$BUNDLE" == "false" ]]; then
            log_info "Skipping $REPO_NAME (bundle=false)"
            continue
        fi
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

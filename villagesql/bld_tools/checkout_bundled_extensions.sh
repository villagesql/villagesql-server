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
# Env vars:
#   EXTENSION_RUNTIME   cpp (default), rust, or all selects which extension list(s)
#                       to read. See extension_lists.sh.

set -euo pipefail

EXTENSION_CLONES_DIR="${1:?Usage: $0 <ext_dir> [<ext_filter>] [include_unbundled]}"
EXTENSION_FILTER="${2:-}"
INCLUDE_UNBUNDLED="${3:-no}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$TOOLS_DIR/extension_lists.sh"

# Which runtime(s) to check out: cpp (default), rust, or all.
EXTENSION_RUNTIME="${EXTENSION_RUNTIME:-cpp}"
EXTENSION_LISTS=()
while IFS= read -r _list; do EXTENSION_LISTS+=("$_list"); done \
    < <(resolve_extension_lists "$EXTENSION_RUNTIME")

case "$INCLUDE_UNBUNDLED" in
    1|yes) INCLUDE_UNBUNDLED=yes ;;
    0|no|"") INCLUDE_UNBUNDLED=no ;;
    *) die "Invalid include_unbundled: $INCLUDE_UNBUNDLED (expected 0/no or 1/yes)" ;;
esac

for _list in "${EXTENSION_LISTS[@]}"; do
    [[ -f "$_list" ]] || die "Extensions list not found: $_list"
done

if [[ ! -d "$EXTENSION_CLONES_DIR" ]]; then
    die "Extension directory not found: $EXTENSION_CLONES_DIR (mkdir first)"
fi

FAILED=0

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

    CLONE_DIR="$EXTENSION_CLONES_DIR/$REPO_NAME"

    # A single repo can back multiple extensions (via path=). Clone it once.
    if [[ -d "$CLONE_DIR" ]]; then
        log_info "Already cloned $REPO_NAME"
        continue
    fi

    log_step "Cloning $REPO_NAME ($SOURCE${BRANCH:+ @ $BRANCH})"


    CLONE_ARGS=(--depth=1)
    [[ -n "$BRANCH" ]] && CLONE_ARGS+=(--branch "$BRANCH")

    # git clone accepts both remote URLs and local filesystem paths
    if ! git clone "${CLONE_ARGS[@]}" "$SOURCE" "$CLONE_DIR" 2>&1; then
        log_error "Failed to clone $SOURCE"
        FAILED=$((FAILED + 1))
        continue
    fi
done < <(cat "${EXTENSION_LISTS[@]}")

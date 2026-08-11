#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Build VEB files for bundled VillageSQL extensions.
#
# Usage: include_bundled_extensions.sh <dst_dir> <veb_src_dir>
#
# <veb_src_dir>: Directory where built .veb files were placed.
#
# Env vars:
#   EXTENSION_RUNTIME   cpp (default), rust, or all - selects which extension
#                       lists to read. See extension_lists.sh.

set -euo pipefail
DST_DIR="${1:?Usage: $0 <dst_dir> <veb_src_dir>}"
VEB_SRC_DIR="${2:?Usage: $0 <dst_dir> <veb_src_dir>}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$TOOLS_DIR/extension_lists.sh"

# Which runtime(s) to include: cpp (default), rust, or all.
EXTENSION_RUNTIME="${EXTENSION_RUNTIME:-cpp}"
EXTENSION_LISTS=()
while IFS= read -r _list; do EXTENSION_LISTS+=("$_list"); done \
    < <(resolve_extension_lists "$EXTENSION_RUNTIME")

for _list in "${EXTENSION_LISTS[@]}"; do
    [[ -f "$_list" ]] || die "Extensions list not found: $_list"
done

if [[ ! -d "$VEB_SRC_DIR" ]]; then
    log_error "Missing extension source directory: $VEB_SRC_DIR"
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

    # Convert all dashes to underscores
    EXT_FILE="${REPO_NAME//-/_}.veb"
    EXT_SRC_VEB="$VEB_SRC_DIR/$EXT_FILE"
    if [[ ! -f "$EXT_SRC_VEB" ]]; then
        log_error "Expected extension $REPO_NAME not found at $EXT_SRC_VEB"
        exit 1
    fi

    cp "$EXT_SRC_VEB" "$DST_DIR/$EXT_FILE" || log_info "Duplicate $REPO_NAME already installed"
done < <(cat "${EXTENSION_LISTS[@]}")

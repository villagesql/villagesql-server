#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Copy the bundled VillageSQL extension VEB files into a package tree.
#
# Usage: include_bundled_extensions.sh <dst_dir> <veb_src_dir>
#
# <veb_src_dir>: Directory where built .veb files were placed.
#
# The manifest is parsed by villagesql/bld_matrix/json_extensions.sh, which
# reads villagesql/dev_server/bundled_extensions.txt from this script's own
# source tree.

set -euo pipefail
DST_DIR="${1:?Usage: $0 <dst_dir> <veb_src_dir>}"
VEB_SRC_DIR="${2:?Usage: $0 <dst_dir> <veb_src_dir>}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

if [[ ! -d "$VEB_SRC_DIR" ]]; then
    die "Missing extension source directory: $VEB_SRC_DIR"
fi

# One tab-separated row per manifest entry. Collected up front, rather than
# piped into the loop, so a manifest error stops the script here.
ENTRIES="$("$SOURCE_DIR/villagesql/bld_matrix/json_extensions.sh" \
    | jq -r '.[] | [.extension, (.bundle | tostring)] | @tsv')"

while IFS=$'\t' read -r EXTENSION BUNDLE; do
    [[ -z "$EXTENSION" ]] && continue

    if [[ "$BUNDLE" == "false" ]]; then
        log_info "Skipping $EXTENSION (bundle=false)"
        continue
    fi

    # Convert all dashes to underscores
    EXT_FILE="${EXTENSION//-/_}.veb"
    EXT_SRC_VEB="$VEB_SRC_DIR/$EXT_FILE"
    if [[ ! -f "$EXT_SRC_VEB" ]]; then
        die "Expected extension $EXTENSION not found at $EXT_SRC_VEB"
    fi

    cp "$EXT_SRC_VEB" "$DST_DIR/$EXT_FILE" || log_info "Duplicate $EXTENSION already installed"
done <<< "$ENTRIES"

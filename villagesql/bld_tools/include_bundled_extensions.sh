#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Copy the bundled VillageSQL extension VEB files into a package tree.
#
# Usage: include_bundled_extensions.sh <dst_dir> <veb_src_dir> [channel]
#
# <dst_dir>:     Directory in the package tree to copy the .veb files into.
# <veb_src_dir>: Directory where built .veb files were placed.
# [channel]:     Build channel to package for: release (default) or dev.
#                "dev" also copies the bundle=dev extensions, which ship only
#                in pre-release artifacts.
#
# The channel must match the one build_bundled_extensions.sh was given: this
# script copies, it does not build, and a .veb the build stage was never asked
# for is a hard error rather than a silently thinner package.
#
# Which entries a channel selects is decided by
# villagesql/bld_matrix/json_bundle_extensions.sh, which reads the manifest at
# villagesql/dev_server/bundled_extensions.txt from this script's own source
# tree.

set -euo pipefail
USAGE="Usage: $0 <dst_dir> <veb_src_dir> [channel]"
DST_DIR="${1:?$USAGE}"
VEB_SRC_DIR="${2:?$USAGE}"
BUNDLE_CHANNEL="${3:-release}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

if [[ ! -d "$VEB_SRC_DIR" ]]; then
    die "Missing extension source directory: $VEB_SRC_DIR"
fi

# One extension per line, those the channel ships. Collected up front, rather
# than piped into the loop, so a manifest error stops the script here.
ENTRIES="$("$SOURCE_DIR/villagesql/bld_matrix/json_bundle_extensions.sh" \
    "$BUNDLE_CHANNEL" | jq -r '.[].extension')"

while IFS= read -r EXTENSION; do
    [[ -z "$EXTENSION" ]] && continue

    # Convert all dashes to underscores
    EXT_FILE="${EXTENSION//-/_}.veb"
    EXT_SRC_VEB="$VEB_SRC_DIR/$EXT_FILE"
    if [[ ! -f "$EXT_SRC_VEB" ]]; then
        die "Expected extension $EXTENSION not found at $EXT_SRC_VEB"
    fi

    cp "$EXT_SRC_VEB" "$DST_DIR/$EXT_FILE" || log_info "Duplicate $EXTENSION already installed"
done <<< "$ENTRIES"

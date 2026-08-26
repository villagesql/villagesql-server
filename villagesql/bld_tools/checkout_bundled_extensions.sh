#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Checkout the sources for bundled VillageSQL extensions.
#
# Usage: checkout_bundled_extensions.sh <ext_dir> [extension] [channel]
#
# <ext_dir>:        Path for checked out extensions.  Should exist before calling.
# [extension]:      Optional extension name to clone only one extension (e.g.
#                   vsql-ai). Omit to clone every extension in the manifest.
# [channel]:        Build channel to clone for: release (default), dev, or test.
#                   "dev" adds the bundle=dev extensions, which ship only in
#                   pre-release artifacts; "test" adds bundle=none too, which
#                   ship nowhere but are still built and tested (e.g. by the
#                   sanitizer workflow). 0/no and 1/yes are accepted as the
#                   old include_unbundled spellings of release and test.
#
# Which entries a channel selects is decided by
# villagesql/bld_matrix/json_bundle_extensions.sh, which reads the manifest at
# villagesql/dev_server/bundled_extensions.txt from this script's own source
# tree.

set -euo pipefail

EXTENSION_CLONES_DIR="${1:?Usage: $0 <ext_dir> [<ext_filter>] [channel]}"
EXTENSION_FILTER="${2:-}"
BUNDLE_CHANNEL="${3:-release}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

if [[ ! -d "$EXTENSION_CLONES_DIR" ]]; then
    die "Extension directory not found: $EXTENSION_CLONES_DIR (mkdir first)"
fi

# One tab-separated row per manifest entry the channel and filter keep.
# Collected up front, rather than piped into the loop, so a manifest error
# stops the script here.
ENTRIES="$("$SOURCE_DIR/villagesql/bld_matrix/json_bundle_extensions.sh" \
    "$BUNDLE_CHANNEL" "$EXTENSION_FILTER" \
    | jq -r '.[] | [.extension, .url, .branch, .build] | @tsv')"

CLONED=0
FAILED=0

while IFS=$'\t' read -r EXTENSION SOURCE BRANCH BUILD_TOOL; do
    [[ -z "$EXTENSION" ]] && continue

    # This script builds with cmake. Skip entries that use another build tool
    # such as cargo. The real issue is that we try to clone the rust-sdk
    # multiple times, leading to an error.
    # TODO(villagesql-rust): Let's consider doing a bigger rework of the
    # extension build system to support multiple build tools, but for now we just
    # skip non-cmake extensions.
    if [[ "$BUILD_TOOL" != "cmake" ]]; then
        log_info "Skipping $EXTENSION (build=$BUILD_TOOL not supported here)"
        continue
    fi

    log_step "Cloning $EXTENSION ($SOURCE @ $BRANCH)"

    # The clone is named for the repo, not the extension, since one repo can
    # hold several extensions. build_bundled_extensions.sh expects that name.
    CLONE_DIR="$EXTENSION_CLONES_DIR/${SOURCE##*/}"

    # git clone accepts both remote URLs and local filesystem paths
    if ! git clone --depth=1 --branch "$BRANCH" "$SOURCE" "$CLONE_DIR" 2>&1; then
        log_error "Failed to clone $SOURCE"
        FAILED=$((FAILED + 1))
        continue
    fi

    CLONED=$((CLONED + 1))
done <<< "$ENTRIES"

echo ""
if [[ -n "$EXTENSION_FILTER" && $CLONED -eq 0 && $FAILED -eq 0 ]]; then
    die "'$EXTENSION_FILTER' matched no extension to clone"
fi
echo "Extensions cloned: $CLONED, failed: $FAILED"
[[ $FAILED -eq 0 ]] || exit 1

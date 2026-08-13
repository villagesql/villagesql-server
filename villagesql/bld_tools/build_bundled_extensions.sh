#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Build VEB files for bundled VillageSQL extensions.
#
# Usage: build_bundled_extensions.sh <ext_dir> <sdk_dir> <veb_output_dir>
#            [extension] [include_unbundled]
# <ext_dir>:        Path to a directory of extension git clones.
# <sdk_dir>:        Path to an extracted villagesql-extension-sdk-* directory.
#                   After 'make', this is $BUILD_DIR/villagesql-extension-sdk-<version>.
# <veb_output_dir>: Directory where built .veb files are placed.
# [extension]:      Optional extension name to build only one extension (e.g.
#                   vsql-ai). Omit to build every extension in the manifest.
# [include_unbundled]: 0/no (default) to skip bundle=false extensions, 1/yes to
#                   build them too. They ship with no dev server, but are still
#                   built and tested (e.g. by the sanitizer workflow).
#
# The manifest is parsed by villagesql/bld_matrix/json_extensions.sh, which
# reads villagesql/dev_server/bundled_extensions.txt from this script's own
# source tree.
#
# Env vars:
#   CMAKE_EXTRA_FLAGS  - additional per-extension cmake flags appended verbatim

set -euo pipefail

USAGE="Usage: $0 <ext_dir> <sdk_dir> <veb_output_dir> [extension] [include_unbundled]"
EXTENSION_CLONES_DIR="${1:?$USAGE}"
SDK_DIR="${2:?$USAGE}"
VEB_OUTPUT_DIR="${3:?$USAGE}"
EXTENSION_FILTER="${4:-}"
INCLUDE_UNBUNDLED="${5:-no}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

case "$INCLUDE_UNBUNDLED" in
    1|yes) INCLUDE_UNBUNDLED=yes ;;
    0|no|"") INCLUDE_UNBUNDLED=no ;;
    *) die "Invalid include_unbundled: $INCLUDE_UNBUNDLED (expected 0/no or 1/yes)" ;;
esac

if [[ ! -d "$SDK_DIR" ]]; then
    die "SDK directory not found: $SDK_DIR"
fi

if [[ ! -d "$EXTENSION_CLONES_DIR" ]]; then
    die "Extension directory not found: $EXTENSION_CLONES_DIR (mkdir first)"
fi

mkdir -p "$VEB_OUTPUT_DIR"

CMAKE_FLAGS=(
    "-DCMAKE_PREFIX_PATH=$SDK_DIR"
    "-DCMAKE_BUILD_TYPE=Release"
)

if [[ -n "${CMAKE_EXTRA_FLAGS:-}" ]]; then
    CMAKE_FLAGS+=($CMAKE_EXTRA_FLAGS)
fi

log_info "CMake flags: ${CMAKE_FLAGS[*]}"

# Build directories are always temporary — they must not land in CLONE_BASE or
# test_extension_vebs.sh will iterate over them as if they were extension repos.
BUILD_BASE="$(mktemp -d)"
trap 'rm -rf "$BUILD_BASE"' EXIT

NCORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo "4")
BUILT=0
FAILED=0

# One tab-separated row per manifest entry the filter keeps. Collected up front,
# rather than piped into the loop, so a manifest error stops the script here.
ENTRIES="$("$SOURCE_DIR/villagesql/bld_matrix/json_extensions.sh" \
    | jq -r --arg f "$EXTENSION_FILTER" '
        .[]
        | select($f == "" or .extension == $f)
        | [.extension, .url, .branch, .build, (.bundle | tostring), .path]
        | @tsv')"

while IFS=$'\t' read -r EXTENSION SOURCE BRANCH BUILD_TOOL BUNDLE EXT_PATH; do
    [[ -z "$EXTENSION" ]] && continue

    # This script builds with cmake. Skip entries that use another build tool
    # such as cargo.
    # TODO(villagesql-rust): Let's consider doing a bigger rework of the
    # extension build system to support multiple build tools, but for now we just
    # skip non-cmake extensions.
    if [[ "$BUILD_TOOL" != "cmake" ]]; then
        log_info "Skipping $EXTENSION (build=$BUILD_TOOL not supported here)"
        continue
    fi

    if [[ "$INCLUDE_UNBUNDLED" == "no" && "$BUNDLE" == "false" ]]; then
        log_info "Skipping $EXTENSION (bundle=false)"
        continue
    fi

    log_step "Building $EXTENSION ($SOURCE @ $BRANCH)"

    # The clone is named for the repo, not the extension, since one repo can
    # hold several extensions; path= says where in the clone this one lives.
    CLONE_DIR="$EXTENSION_CLONES_DIR/${SOURCE##*/}${EXT_PATH:+/$EXT_PATH}"
    EXT_BUILD_DIR="$BUILD_BASE/$EXTENSION"

    if ! cmake -S "$CLONE_DIR" -B "$EXT_BUILD_DIR" \
            "${CMAKE_FLAGS[@]}" 2>&1; then
        log_error "CMake configure failed for $EXTENSION"
        FAILED=$((FAILED + 1))
        continue
    fi

    if ! cmake --build "$EXT_BUILD_DIR" --parallel "$NCORES" 2>&1; then
        log_error "Build failed for $EXTENSION"
        FAILED=$((FAILED + 1))
        continue
    fi

    VEB_COUNT=0
    while IFS= read -r veb; do
        cp "$veb" "$VEB_OUTPUT_DIR/"
        log_info "$(basename "$veb")"
        VEB_COUNT=$((VEB_COUNT + 1))
    done < <(find "$EXT_BUILD_DIR" -maxdepth 1 -name "*.veb")

    if [[ $VEB_COUNT -eq 0 ]]; then
        log_error "No .veb file found after building $EXTENSION"
        FAILED=$((FAILED + 1))
        continue
    fi

    BUILT=$((BUILT + 1))
done <<< "$ENTRIES"

echo ""
if [[ -n "$EXTENSION_FILTER" && $BUILT -eq 0 && $FAILED -eq 0 ]]; then
    die "'$EXTENSION_FILTER' matched no extension to build"
fi
echo "Extensions built: $BUILT, failed: $FAILED"
[[ $FAILED -eq 0 ]] || exit 1

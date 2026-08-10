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
# [extension]:      Optional repo name to build only one extension (e.g. vsql-ai).
#                   Omit to build all extensions in bundled_extensions.txt.
# [include_unbundled]: 0/no (default) to skip bundle=false extensions, 1/yes to
#                   build them too. They ship with no dev server, but are still
#                   built and tested (e.g. by the sanitizer workflow).
#
# The extension list is read from villagesql/dev_server/bundled_extensions.txt,
# located relative to this script's source tree.
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

EXTENSIONS_LIST="$SOURCE_DIR/villagesql/dev_server/bundled_extensions.txt"

source "$SOURCE_DIR/villagesql/bld_tools/vsql_script_utils.sh"

case "$INCLUDE_UNBUNDLED" in
    1|yes) INCLUDE_UNBUNDLED=yes ;;
    0|no|"") INCLUDE_UNBUNDLED=no ;;
    *) die "Invalid include_unbundled: $INCLUDE_UNBUNDLED (expected 0/no or 1/yes)" ;;
esac

if [[ ! -d "$SDK_DIR" ]]; then
    log_error "SDK directory not found: $SDK_DIR"
    exit 1
fi

if [[ ! -f "$EXTENSIONS_LIST" ]]; then
    log_error "Extensions list not found: $EXTENSIONS_LIST"
    exit 1
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

    # This script builds with cmake. Skip entries that use another build tool
    # such as cargo. The real issue is that we try to clone the rust-sdk
    # multiple times, leading to an error.
    # TODO(villagesql-rust): Let's consider doing a bigger rework of the
    # extension build system to support multiple build tools, but for now we just
    # skip non-cmake extensions.
    BUILD_TOOL=cmake
    for FIELD in "${FIELDS[@]:2}"; do
        [[ "$FIELD" == build=* ]] && BUILD_TOOL="${FIELD#build=}"
    done
    if [[ "$BUILD_TOOL" != "cmake" ]]; then
        log_info "Skipping $REPO_NAME (build=$BUILD_TOOL not supported here)"
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

    log_step "Building $REPO_NAME ($SOURCE${BRANCH:+ @ $BRANCH})"

    CLONE_DIR="$EXTENSION_CLONES_DIR/$REPO_NAME"
    EXT_BUILD_DIR="$BUILD_BASE/$REPO_NAME"

    if ! cmake -S "$CLONE_DIR" -B "$EXT_BUILD_DIR" \
            "${CMAKE_FLAGS[@]}" 2>&1; then
        log_error "CMake configure failed for $REPO_NAME"
        FAILED=$((FAILED + 1))
        continue
    fi

    if ! cmake --build "$EXT_BUILD_DIR" --parallel "$NCORES" 2>&1; then
        log_error "Build failed for $REPO_NAME"
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
        log_error "No .veb file found after building $REPO_NAME"
        FAILED=$((FAILED + 1))
        continue
    fi

    BUILT=$((BUILT + 1))
done < "$EXTENSIONS_LIST"

echo ""
if [[ -n "$EXTENSION_FILTER" && $BUILT -eq 0 && $FAILED -eq 0 ]]; then
    log_error "'$EXTENSION_FILTER' not found in $EXTENSIONS_LIST"
    exit 1
fi
echo "Extensions built: $BUILT, failed: $FAILED"
[[ $FAILED -eq 0 ]] || exit 1

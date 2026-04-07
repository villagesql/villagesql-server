#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Build VEB files for bundled VillageSQL extensions.
#
# Usage: build_bundled_extensions.sh <sdk_dir> <veb_output_dir> [extension]
#
# <sdk_dir>:        Path to an extracted villagesql-extension-sdk-* directory.
#                   After 'make', this is $BUILD_DIR/villagesql-extension-sdk-<version>.
# <veb_output_dir>: Directory where built .veb files are placed.
# [extension]:      Optional repo name to build only one extension (e.g. vsql-ai).
#                   Omit to build all extensions in bundled_extensions.txt.
#
# The extension list is read from villagesql/dev_server/bundled_extensions.txt,
# located relative to this script's source tree.

set -euo pipefail

SDK_DIR="${1:?Usage: $0 <sdk_dir> <veb_output_dir> [extension]}"
VEB_OUTPUT_DIR="${2:?Usage: $0 <sdk_dir> <veb_output_dir> [extension]}"
EXTENSION_FILTER="${3:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/vsql_script_utils.sh"
EXTENSIONS_LIST="$SOURCE_DIR/villagesql/dev_server/bundled_extensions.txt"

if [[ ! -d "$SDK_DIR" ]]; then
    log_error "SDK directory not found: $SDK_DIR"
    exit 1
fi

if [[ ! -f "$EXTENSIONS_LIST" ]]; then
    log_error "Extensions list not found: $EXTENSIONS_LIST"
    exit 1
fi

mkdir -p "$VEB_OUTPUT_DIR"

# Cloned sources go into EXTENSION_CLONES_DIR (if set) so callers such as
# test_extension_vebs.sh can access mysql-test/ directories after this script
# exits.  Otherwise a temp dir is used and cleaned up on exit.
if [[ -n "${EXTENSION_CLONES_DIR:-}" ]]; then
    CLONE_BASE="$EXTENSION_CLONES_DIR"
    mkdir -p "$CLONE_BASE"
else
    CLONE_BASE="$(mktemp -d)"
    trap 'rm -rf "$CLONE_BASE"' EXIT
fi

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

    # Parse "url [branch-or-tag]" — branch is optional
    read -r SOURCE BRANCH <<< "$line"
    SOURCE="${SOURCE%/}"  # strip any trailing slash
    REPO_NAME="${SOURCE##*/}"

    if [[ -n "$EXTENSION_FILTER" && "$REPO_NAME" != "$EXTENSION_FILTER" ]]; then
        continue
    fi

    log_step "Building $REPO_NAME ($SOURCE${BRANCH:+ @ $BRANCH})"

    CLONE_DIR="$CLONE_BASE/$REPO_NAME"
    EXT_BUILD_DIR="$BUILD_BASE/$REPO_NAME"

    CLONE_ARGS=(--depth=1)
    [[ -n "$BRANCH" ]] && CLONE_ARGS+=(--branch "$BRANCH")

    # git clone accepts both remote URLs and local filesystem paths
    if ! git clone "${CLONE_ARGS[@]}" "$SOURCE" "$CLONE_DIR" 2>&1; then
        log_error "Failed to clone $SOURCE"
        FAILED=$((FAILED + 1))
        continue
    fi

    if ! cmake -S "$CLONE_DIR" -B "$EXT_BUILD_DIR" \
            -DCMAKE_PREFIX_PATH="$SDK_DIR" \
            -DCMAKE_BUILD_TYPE=Release 2>&1; then
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

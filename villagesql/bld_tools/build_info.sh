#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Shared utilities for VillageSQL build scripts.
# Source this file; do not execute directly.
#
# Usage:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/build_info.sh"
#
# Expects a prior inclusion of the utities
#   source "$SOURCE_DIR/villagesql/scripts/vsql_scripts_utils.sh"

# Parse VSQL_VERSION file from <source_dir>.
# Sets VSQL_MAJOR, VSQL_MINOR, VSQL_PATCH, VSQL_PRE, VSQL_VERSION.
vsql_parse_version() {
    local source_dir="$1"
    local f="$source_dir/VSQL_VERSION"
    [[ -f "$f" ]] || die "VSQL_VERSION file not found at $f"
    VSQL_CODE_BASE=$(grep "^VSQL_CODE_BASE=" "$f" | cut -d'=' -f2)
    VSQL_MAJOR=$(grep "^VSQL_MAJOR_VERSION=" "$f" | cut -d'=' -f2)
    VSQL_MINOR=$(grep "^VSQL_MINOR_VERSION=" "$f" | cut -d'=' -f2)
    VSQL_PATCH=$(grep "^VSQL_PATCH_VERSION=" "$f" | cut -d'=' -f2)
    # VSQL_PRE_RELEASE_VERSION env var overrides the file (set to "" to strip the suffix)
    if [[ "${VSQL_PRE_RELEASE_VERSION+set}" == "set" ]]; then
        VSQL_PRE="$VSQL_PRE_RELEASE_VERSION"
    else
        VSQL_PRE=$(grep "^VSQL_PRE_RELEASE_VERSION=" "$f" | cut -d'=' -f2)
    fi
    VSQL_VERSION="${VSQL_MAJOR}.${VSQL_MINOR}.${VSQL_PATCH}"
    if [[ -n "$VSQL_PRE" ]]; then
        VSQL_VERSION="${VSQL_VERSION}-${VSQL_PRE}"
    fi
}

# Print the VillageSQL version of <source_dir> as <major>.<minor>.<patch>,
# with -<pre_release> appended when there is one.
#
# Usage: vsql_json_version <source_dir> [pre_release]
#   The version comes from villagesql/bld_matrix/json_version.sh. With no
#   pre_release argument the file's own suffix is used; passing one replaces it,
#   and passing "" prints the version with no suffix, as a release build wants.
#
# Prints to stdout and sets nothing, so a caller captures it:
#   VSQL_VERSION=$(vsql_json_version "$SOURCE_DIR")
vsql_json_version() {
    local source_dir="$1"
    local json
    json=$("$source_dir/villagesql/bld_matrix/json_version.sh") \
        || die "Cannot read the version of $source_dir"

    local pre
    if [[ $# -ge 2 ]]; then
        pre="$2"
    else
        pre=$(jq -r '.pre_release' <<<"$json")
    fi

    local version
    version=$(jq -r '"\(.major).\(.minor).\(.patch)"' <<<"$json")
    if [[ -n "$pre" ]]; then
        version="${version}-${pre}"
    fi
    echo "$version"
}

# Set PLATFORM (linux|macos) and ARCH (x86_64|aarch64|arm64) for this machine.
vsql_platform_info() {
    PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')"
    [[ "$PLATFORM" == "darwin" ]] && PLATFORM="macos"
    ARCH="$(uname -m)"
}

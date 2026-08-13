#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Shared utilities for VillageSQL build scripts.
# Source this file; do not execute directly.
#
# Usage:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/build_info.sh"

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

# Set PLATFORM (linux|macos) and ARCH (x86_64|aarch64|arm64) for this machine.
vsql_platform_info() {
    PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')"
    [[ "$PLATFORM" == "darwin" ]] && PLATFORM="macos"
    ARCH="$(uname -m)"
}

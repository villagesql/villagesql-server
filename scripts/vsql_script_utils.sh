#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Shared utilities for VillageSQL build scripts.
# Source this file; do not execute directly.
#
# Usage:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/vsql_script_utils.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_step()  { echo -e "${BLUE}==>${NC} $*"; }
die()       { log_error "$*"; exit 1; }

# Parse VSQL_VERSION file from <source_dir>.
# Sets VSQL_MAJOR, VSQL_MINOR, VSQL_PATCH, VSQL_PRE, VSQL_VERSION.
vsql_parse_version() {
    local source_dir="$1"
    local f="$source_dir/VSQL_VERSION"
    [[ -f "$f" ]] || die "VSQL_VERSION file not found at $f"
    VSQL_MAJOR=$(grep "^VSQL_MAJOR_VERSION=" "$f" | cut -d'=' -f2)
    VSQL_MINOR=$(grep "^VSQL_MINOR_VERSION=" "$f" | cut -d'=' -f2)
    VSQL_PATCH=$(grep "^VSQL_PATCH_VERSION=" "$f" | cut -d'=' -f2)
    VSQL_PRE=$(grep "^VSQL_PRE_RELEASE_VERSION=" "$f" | cut -d'=' -f2)
    VSQL_VERSION="${VSQL_MAJOR}.${VSQL_MINOR}.${VSQL_PATCH}"
    [[ -n "$VSQL_PRE" ]] && VSQL_VERSION="${VSQL_VERSION}-${VSQL_PRE}"
}

# Set PLATFORM (linux|macos) and ARCH (x86_64|aarch64|arm64) for this machine.
vsql_platform_info() {
    PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')"
    [[ "$PLATFORM" == "darwin" ]] && PLATFORM="macos"
    ARCH="$(uname -m)"
}

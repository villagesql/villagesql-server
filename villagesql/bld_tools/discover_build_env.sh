#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Detect the operating-system family of the current host and print a canonical
# name on stdout. Used by setup_build_env.sh to pick the correct per-OS
# configuration script.
#
# Prints exactly one of: linux, macos, windows, cygwin, unknown
# Always exits 0; callers inspect the printed value rather than the exit code.
#
# This script can be either executed (prints the value) or sourced (defines
# discover_build_env for use by other scripts).

discover_build_env() {
    # uname is present on Linux, macOS, Cygwin, and Git-Bash/MSYS, so prefer it.
    local kernel=""
    if command -v uname >/dev/null 2>&1; then
        kernel="$(uname -s 2>/dev/null)"
    fi

    case "$kernel" in
        Linux*)                   echo "linux";   return ;;
        Darwin*)                  echo "macos";   return ;;
        CYGWIN*)                  echo "cygwin";  return ;;
        MINGW*|MSYS*|Windows_NT)  echo "windows"; return ;;
    esac

    # Fall back to environment markers when uname is unavailable (e.g. a bare
    # Windows command shell).
    case "${OS:-}" in
        Windows_NT) echo "windows"; return ;;
    esac
    case "${OSTYPE:-}" in
        linux*)       echo "linux";   return ;;
        darwin*)      echo "macos";   return ;;
        cygwin*)      echo "cygwin";  return ;;
        msys*|win32)  echo "windows"; return ;;
    esac

    echo "unknown"
}

# When executed directly (not sourced), print the detected value.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    discover_build_env
fi

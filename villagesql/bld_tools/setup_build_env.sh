#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Set up the build environment for VillageSQL Server on a given operating
# system by dispatching to the matching setup_<os>_build_env.sh script in this
# directory.
#
# Usage:
#   setup_build_env.sh [os]
#
# where os is one of: linux, macos. When omitted, the OS is auto-detected via
# discover_build_env.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    echo "Usage: $(basename "$0") [linux|macos]" >&2
}

main() {
    # Canonicalize the OS name to lowercase so callers can pass any casing,
    # e.g. GitHub Actions' runner.os ("Linux", "macOS", "Windows").
    os="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
    if [[ -z "$os" ]]; then
        os="$("$SCRIPT_DIR/discover_build_env.sh")"
        echo "==> Auto-detected OS: $os" >&2
    fi

    case "$os" in
        linux|macos)
            ;;
        *)
            echo "[ERROR] Unsupported OS: '$os'" >&2
            usage
            exit 1
            ;;
    esac

    config_script="$SCRIPT_DIR/setup_${os}_build_env.sh"
    if [[ ! -x "$config_script" ]]; then
        echo "[ERROR] Configuration script not found or not executable: $config_script" >&2
        exit 1
    fi

    echo "==> Setting up $os build environment" >&2
    exec "$config_script"
}

main "$@"

#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Build the VillageSQL server for CI, adding the CMake flags the host platform
# needs, then delegate to build_ci.sh.
#
# build_ci.sh is deliberately platform-agnostic: it configures and builds with
# whatever flags it is handed. The per-platform decisions live here instead, so
# that every workflow which builds on more than one platform makes the same
# ones. Workflows pinned to a single Linux runner may still call build_ci.sh
# directly; anything that can land on macOS should come through here.
#
# The host OS is detected, not passed in, so a caller that already knows which
# runner it dispatched to cannot disagree with the machine it landed on.
#
# Env vars:
#   SOURCE_DIR, BUILD_DIR, BUILD_TYPE, PARALLEL_JOBS
#       Not read here; passed through to build_ci.sh, which documents them.
#   CMAKE_EXTRA_FLAGS
#       Caller-supplied cmake flags. Platform flags are appended after these,
#       so a caller cannot accidentally override a platform requirement.
#   VSQL_PRE_RELEASE_VERSION
#       Forwarded only when the variable is *set*, which includes being set to
#       the empty string: empty means "release build, drop the -dev suffix".
#       Leaving it unset is what preserves the default -dev label, so an unset
#       variable must not turn into an empty -D flag. This is the same
#       set-versus-unset distinction build_info.sh makes.

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$TOOLS_DIR/discover_build_env.sh"

EXTRA_FLAGS="${CMAKE_EXTRA_FLAGS:-}"

OS="$(discover_build_env)"
case "$OS" in
    macos)
        EXTRA_FLAGS="$EXTRA_FLAGS -DWITH_SSL=$(brew --prefix openssl)"
        ;;
esac

if [[ "${VSQL_PRE_RELEASE_VERSION+set}" == "set" ]]; then
    EXTRA_FLAGS="$EXTRA_FLAGS -DVSQL_PRE_RELEASE_VERSION=${VSQL_PRE_RELEASE_VERSION}"
fi

EXTRA_FLAGS="${EXTRA_FLAGS# }"
echo "==> Building for $OS; extra cmake flags: ${EXTRA_FLAGS:-(none)}" >&2

export CMAKE_EXTRA_FLAGS="$EXTRA_FLAGS"
"$TOOLS_DIR/build_ci.sh"

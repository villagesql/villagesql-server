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
#
# On macOS, ccache is used whenever it is installed. The macOS runners are
# persistent machines whose workspace is wiped before every job, so a
# machine-local cache is what carries compile work from one job to the next.
# Linux runners are ephemeral VMs; their workflows wire up ccache themselves,
# backed by the GitHub Actions cache. Where the macOS cache lives and how big
# it may grow is the runner's configuration (CCACHE_DIR, CCACHE_MAXSIZE).

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$TOOLS_DIR/discover_build_env.sh"

EXTRA_FLAGS="${CMAKE_EXTRA_FLAGS:-}"
USE_CCACHE=0

OS="$(discover_build_env)"
case "$OS" in
    macos)
        # Pin openssl@3: Homebrew retargeted the unversioned `openssl`
        # alias to openssl@4, which MySQL 8.4 does not build against.
        EXTRA_FLAGS="$EXTRA_FLAGS -DWITH_SSL=$(brew --prefix openssl@3)"
        if command -v ccache >/dev/null 2>&1; then
            EXTRA_FLAGS="$EXTRA_FLAGS -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
            USE_CCACHE=1
        fi
        ;;
esac

if [[ "${VSQL_PRE_RELEASE_VERSION+set}" == "set" ]]; then
    EXTRA_FLAGS="$EXTRA_FLAGS -DVSQL_PRE_RELEASE_VERSION=${VSQL_PRE_RELEASE_VERSION}"
fi

EXTRA_FLAGS="${EXTRA_FLAGS# }"
echo "==> Building for $OS; extra cmake flags: ${EXTRA_FLAGS:-(none)}" >&2

export CMAKE_EXTRA_FLAGS="$EXTRA_FLAGS"

# Zero the statistics first so the numbers printed afterwards describe this
# build alone, and print them even when the build fails.
if (( USE_CCACHE )); then
    ccache --zero-stats
fi
rc=0
"$TOOLS_DIR/build_ci.sh" || rc=$?
if (( USE_CCACHE )); then
    echo "==> ccache statistics for this build:" >&2
    ccache --show-stats
fi
exit "$rc"

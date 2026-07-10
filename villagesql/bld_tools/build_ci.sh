#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Configure and build the VillageSQL server for CI.
#
# Env vars:
#   BUILD_DIR          - build output directory (default: <source>/../build)
#   SOURCE_DIR         - source root (default: parent of script dir)
#   BUILD_TYPE         - debug or release (default: release, uses RelWithDebInfo)
#   PARALLEL_JOBS      - parallel make jobs (default: auto-detected)
#   CMAKE_EXTRA_FLAGS  - additional cmake flags appended verbatim
#
# Control pre-release version naming by including "-DVSQL_PRE_RELEASE_VERSION="
# in the CMAKE_EXTRA_FLAGS variable. A blank value as above will remove the
# default '-dev' prerelease version label.

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$TOOLS_DIR/../scripts" && pwd)"
source "$SCRIPTS_DIR/vsql_script_utils.sh"

SOURCE_DIR="${SOURCE_DIR:-$(cd "$SCRIPTS_DIR/../.." && pwd)}"
BUILD_DIR="${BUILD_DIR:-$(cd "$SOURCE_DIR/.." && pwd)/build}"
BUILD_TYPE="${BUILD_TYPE:-release}"
PARALLEL_JOBS="${PARALLEL_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo "4")}"

log_step "VillageSQL CI Build"
echo ""

# Verify source directory exists
if [[ ! -d "$SOURCE_DIR" ]]; then
    die "Source directory not found: $SOURCE_DIR"
fi
if [[ ! -f "$SOURCE_DIR/CMakeLists.txt" ]]; then
    die "Source directory doesn't appear to be valid (no CMakeLists.txt): $SOURCE_DIR"
fi

mkdir -p "$BUILD_DIR"

log_info "Source Directory: $SOURCE_DIR"
log_info "Build Directory:  $BUILD_DIR"
log_info "Build Type:       $BUILD_TYPE"
log_info "Parallel Jobs:    $PARALLEL_JOBS"
echo ""

log_step "Step 1: Configuring build with CMake..."
cd "$BUILD_DIR"

CMAKE_FLAGS=(
    "-DWITH_SSL=system"
)

if [[ "$BUILD_TYPE" == "debug" ]]; then
    CMAKE_FLAGS+=("-DCMAKE_BUILD_TYPE=Debug" "-DWITH_DEBUG=1")
else
    CMAKE_FLAGS+=("-DCMAKE_BUILD_TYPE=RelWithDebInfo")
fi

if [[ -n "${CMAKE_EXTRA_FLAGS:-}" ]]; then
    CMAKE_FLAGS+=($CMAKE_EXTRA_FLAGS)
fi

log_info "CMake flags: ${CMAKE_FLAGS[*]}"
cmake "$SOURCE_DIR" "${CMAKE_FLAGS[@]}" || die "CMake configuration failed"
log_info "CMake configuration complete"

log_step "Step 2: Building binaries..."
log_info "Building with $PARALLEL_JOBS parallel jobs..."
make -j"${PARALLEL_JOBS}" || die "Build failed"

if [[ ! -x "$BUILD_DIR/runtime_output_directory/mysqld" ]]; then
    die "mysqld not found in $BUILD_DIR/runtime_output_directory/ after build"
fi
log_info "Build complete"

log_step "Step 3: Building unit tests..."
make -j"${PARALLEL_JOBS}" villagesql-unit-tests || die "Unit test build failed"
log_info "Unit tests built"

log_step "Build succeeded: $BUILD_DIR/runtime_output_directory/mysqld"

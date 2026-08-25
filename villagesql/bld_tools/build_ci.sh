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
SOURCE_DIR="${SOURCE_DIR:-$(cd "$TOOLS_DIR/../.." && pwd)}"

BUILD_DIR="${BUILD_DIR:-$(cd "$SOURCE_DIR/.." && pwd)/build}"
BUILD_TYPE="${BUILD_TYPE:-release}"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

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
    # Pin curl instead of letting it default. WITH_CURL_DEFAULT is "none" unless
    # internal/CMakeLists.txt exists (CMakeLists.txt:958), and Percona's
    # components/keyrings/keyring_vault/CMakeLists.txt:33 is a FATAL_ERROR when
    # CURL is not found, so an unpinned build can fail configure outright:
    #
    #   -- WITH_CURL=none, not using any curl library.
    #   CMake Error at components/keyrings/keyring_vault/CMakeLists.txt:33:
    #     Not building Keyring Vault Component, could not find CURL library
    #
    # That is what killed the macos-arm64 job of run 32797951446 while the
    # linux jobs of the same run passed on byte-identical flags. The macOS
    # runner was self-hosted and reused its build dir, and this value is
    # cache-sticky -- upstream says so itself at CMakeLists.txt:1941, "WITH_CURL
    # may be set to 'none' in the cache". Pinning it makes the outcome the same
    # on a warm workspace and a cold one.
    #
    # Not the same thing as `brew install curl`: brew's curl is keg-only on
    # macOS, and the build that did work found the Xcode SDK's libcurl, not
    # brew's.
    "-DWITH_CURL=system"
    # The Percona 8.4.10 merge brings in Percona's .gitmodules, which declares
    # three submodules we do not vendor and CI does not clone:
    #
    #   storage/rocksdb/rocksdb  -> percona/rocksdb        (MyRocks)
    #   extra/coredumper         -> Percona-Lab/coredumper
    #   extra/libkmip            -> Percona-Lab/libkmip    (KMIP keyring)
    #
    # All three default ON, so without these flags configure fails hunting for
    # sources that were never checked out ("does not contain a CMakeLists.txt",
    # "build_version.cc.in does not exist").
    #
    # Turning them off is a deliberate scope decision, not just a build fix:
    # enabling MyRocks et al. means vendoring three new dependencies and taking
    # on the rocksdb/percona_innodb test suites. Revisit as its own change.
    #
    # Most of the affected tests notice the missing feature and skip on their
    # own -- the rocksdb suite does, and component_keyring_kmip does once
    # dynamic_loading.test guards on the KMIP component instead of the file one
    # (it was the only one of the 28 that did not skip). percona.coredump is
    # the remaining exception: it guards only on ARM/valgrind/ASAN, so with
    # coredumper compiled out it execs mysqld --coredumper, never matches the
    # pattern it greps for, and hangs to the 900s timeout -- three times over,
    # with retries. It still needs a guard of its own; there is no
    # include/have_coredumper.inc to source, and --coredumper is registered
    # unconditionally in mysqld.cc so its presence proves nothing.
    #
    # TODO(villagesql-rebase): reenable in CI
    "-DWITH_ROCKSDB=0"
    "-DWITH_COREDUMPER=OFF"
    "-DWITHOUT_COMPONENT_KEYRING_KMIP=ON"
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

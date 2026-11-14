#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Lay out a previously-built VillageSQL development server as a directory
# tree: the same tree that a villagesql-dev-server-* tarball unpacks to, with
# the server and client binaries under bin/, the example (and optionally the
# bundled) extensions under lib/veb/, the stripped mysql-test framework, the
# villagesql control script, QUICKSTART.md, and VERSION.
#
# package_dev_server.sh calls this and then tars the result; build_dev_server.sh
# calls this to install straight into a directory. It assumes the server has
# already been built in BUILD_DIR (e.g. by build_server.sh) and, when
# BUILD_BUNDLED_EXTENSIONS=1, that the bundled extension VEBs have already been
# built into $BUILD_DIR/veb_output_directory (by build_bundled_extensions.sh).
#
# Usage: stage_dev_server.sh <dest_dir>
#
# <dest_dir>: Directory to populate. Created if missing; must be empty.
#
# Environment:
#   BUILD_DIR   Directory containing the completed build
#               (default: <repo-parent>/build)
#   SOURCE_DIR  Source root (default: the tree this script lives in)
#   BUILD_BUNDLED_EXTENSIONS
#               "1" to fold bundled extension VEBs into lib/veb
#   INCLUDE_SDK "1" to copy the extension SDK directory
#               (villagesql-extension-sdk-<version>) into the tree, where the
#               install script expects it next to bin/. The tarball does not
#               carry it; the SDK is released as its own artifact.
#   VSQL_PRE_RELEASE_VERSION
#               When set, overrides the pre-release suffix from VSQL_VERSION
#               (set to "" for a release build). Must match what the build was
#               configured with, since the SDK directory is named by it.

set -euo pipefail

DEST_DIR="${1:?Usage: $0 <dest_dir>}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${SOURCE_DIR:-$(cd "$TOOLS_DIR/../.." && pwd)}"
BUILD_DIR="${BUILD_DIR:-$(cd "$SOURCE_DIR/.." && pwd)/build}"
BUILD_BUNDLED_EXTENSIONS="${BUILD_BUNDLED_EXTENSIONS:-0}"
INCLUDE_SDK="${INCLUDE_SDK:-0}"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"
source "$TOOLS_DIR/build_info.sh"
vsql_parse_version "$SOURCE_DIR"
vsql_platform_info

TEMPLATE_DIR="$SOURCE_DIR/villagesql/dev_server"
BASE_TARBALL_STEM="villagesql-base-temp"
BASE_TARBALL="$BUILD_DIR/$BASE_TARBALL_STEM.tar.gz"
CPACK_LOG="$BUILD_DIR/$BASE_TARBALL_STEM.cpack.log"
SSL_KEEP_DIR=""

cleanup() {
    rm -f "$BASE_TARBALL"
    [[ -n "$SSL_KEEP_DIR" ]] && rm -rf "$SSL_KEEP_DIR"
    return 0
}
trap cleanup EXIT

[[ -d "$BUILD_DIR" ]] || die "Build directory not found: $BUILD_DIR (build the server first)"
[[ -x "$BUILD_DIR/runtime_output_directory/mysqld" ]] \
    || die "mysqld not found in $BUILD_DIR/runtime_output_directory (build the server first)"

mkdir -p "$DEST_DIR"
DEST_DIR="$(cd "$DEST_DIR" && pwd)"
if [[ -n "$(ls -A "$DEST_DIR")" ]]; then
    die "Destination is not empty: $DEST_DIR"
fi

log_info "VillageSQL Version: $VSQL_VERSION"
log_info "Platform: $PLATFORM-$ARCH"
log_info "Build Directory: $BUILD_DIR"
log_info "Destination: $DEST_DIR"
log_info "Tree includes:"
log_info "  - Server and client binaries"
log_info "  - Example VEB files (vsql_simple, vsql_complex)"
if [[ "$BUILD_BUNDLED_EXTENSIONS" == "1" ]]; then
    log_info "  - Bundled extensions (from villagesql/dev_server/bundled_extensions.txt)"
fi
if [[ "$INCLUDE_SDK" == "1" ]]; then
    log_info "  - Extension SDK"
fi
log_info "  - mysql-test framework (binaries only, no test/result files)"
log_info "  - Support files and SQL scripts"
echo ""

# CPack runs the "preinstall" target first, which is "make all" with no
# parallelism and its output hidden in the log. If anything made the tree stale
# since the last build, a new CMake for instance, that is a full rebuild on one
# core that looks like a hang. Do the same work here, in parallel and visibly,
# so CPack finds nothing left to do.
log_step "Step 0: Bringing the build up to date..."
PARALLEL_JOBS="${PARALLEL_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo "4")}"
cmake --build "$BUILD_DIR" --parallel "$PARALLEL_JOBS" || die "Build is not up to date and could not be rebuilt"

log_step "Step 1: Generating base package with CPack..."
log_info "CPack log: $CPACK_LOG"

CPACK_COMPONENTS="Client;Server;Server_Scripts;SharedLibraries;SupportFiles;Readme;Info;ExampleVebs;Test;TestReadme"
log_info "CPack components: $CPACK_COMPONENTS"

# CPack writes its archive into the current directory.
(
    cd "$BUILD_DIR"
    cpack -G TGZ \
        -D CPACK_ARCHIVE_COMPONENT_INSTALL=ON \
        -D CPACK_COMPONENTS_GROUPING=ALL_COMPONENTS_IN_ONE \
        -D "CPACK_COMPONENTS_ALL=${CPACK_COMPONENTS}" \
        -D CPACK_PACKAGE_FILE_NAME="$BASE_TARBALL_STEM" \
        >"$CPACK_LOG" 2>&1
) || {
    tail -30 "$CPACK_LOG" >&2
    die "CPack failed (log: $CPACK_LOG). Check that the build is complete."
}
[[ -f "$BASE_TARBALL" ]] || die "CPack did not create expected tarball: $BASE_TARBALL"
rm -f "$CPACK_LOG"

log_info "Base package size: $(du -h "$BASE_TARBALL" | cut -f1)"

log_step "Step 2: Extracting base package..."
# CPack creates tarballs without a wrapping directory, so extract in place.
cd "$DEST_DIR"
tar xzf "$BASE_TARBALL"

log_step "Step 3: Stripping unnecessary test data..."
if [[ -d "mysql-test" ]]; then
    if [[ -d "mysql-test/std_data" ]]; then
        log_info "Removing unnecessary test data from std_data..."
        # Keep only the standard CA and server/client certificate pairs, which
        # work together; the rest of std_data is test fixtures.
        SSL_KEEP_DIR="$(mktemp -d)"
        for file in cacert.pem \
                    server-cert.pem server-key.pem \
                    client-cert.pem client-key.pem; do
            if [[ -f "mysql-test/std_data/$file" ]]; then
                cp "mysql-test/std_data/$file" "$SSL_KEEP_DIR/"
            fi
        done

        rm -rf mysql-test/std_data
        mkdir -p mysql-test/std_data
        if [[ -n "$(ls -A "$SSL_KEEP_DIR")" ]]; then
            mv "$SSL_KEEP_DIR"/* mysql-test/std_data/
        fi
        rm -rf "$SSL_KEEP_DIR"
        SSL_KEEP_DIR=""

        STD_DATA_SIZE=$(du -sh mysql-test/std_data 2>/dev/null | cut -f1 || echo "unknown")
        log_info "Preserved essential SSL certificates in std_data ($STD_DATA_SIZE)"
    fi

    log_info "Removing all test suites and test/result files..."
    rm -rf mysql-test/suite mysql-test/r mysql-test/t

    MYSQL_TEST_SIZE=$(du -sh mysql-test 2>/dev/null | cut -f1 || echo "unknown")
    log_info "mysql-test framework size after cleanup: $MYSQL_TEST_SIZE"
fi

if [[ "$BUILD_BUNDLED_EXTENSIONS" == "1" ]]; then
    log_step "Step 4: Adding bundled extensions..."
    mkdir -p "lib/veb"
    "$TOOLS_DIR/include_bundled_extensions.sh" \
            "lib/veb" \
            "$BUILD_DIR/veb_output_directory"
    log_info "Bundled extensions added"
else
    log_info "Step 4: Skipping bundled extensions (set BUILD_BUNDLED_EXTENSIONS=1 to include)"
fi

if [[ "$INCLUDE_SDK" == "1" ]]; then
    log_step "Step 5: Adding extension SDK..."
    SDK_DIR="$("$TOOLS_DIR/get_sdk.sh" "$BUILD_DIR")"
    [[ -d "$SDK_DIR" ]] || die "Extension SDK not found at $SDK_DIR (run 'make sdk' in $BUILD_DIR)"
    cp -R "$SDK_DIR" "$DEST_DIR/"
    log_info "Extension SDK added as $(basename "$SDK_DIR")"
else
    log_info "Step 5: Skipping extension SDK (set INCLUDE_SDK=1 to include)"
fi

log_step "Step 6: Adding convenience scripts..."
cp "$TEMPLATE_DIR/villagesql.sh" villagesql && chmod +x villagesql
[[ -f "villagesql" ]] || die "Failed to copy villagesql from $TEMPLATE_DIR"
log_info "Convenience scripts added"

log_step "Step 7: Creating documentation..."

# Generate test documentation (always included with stripped MySQL tests)
TEST_NOTE="**Note:** This package includes the test framework (mysql-test-run.pl, lib/, include/) without any bundled test suites. You can create your own test suites for your extensions."
sed "s|@TEST_NOTE@|$TEST_NOTE|g" "$TEMPLATE_DIR/TEST_DOCS.md.template" > test_docs.tmp

# Process QUICKSTART template
sed -e "s|@VSQL_VERSION@|$VSQL_VERSION|g" \
    -e "s|@PLATFORM@|$PLATFORM|g" \
    -e "s|@ARCH@|$ARCH|g" \
    -e "s|@BUILD_DATE@|$(date -u +"%Y-%m-%d")|g" \
    "$TEMPLATE_DIR/QUICKSTART.md.template" > QUICKSTART.md.tmp

# Insert test documentation at the @TEST_DOCS@ marker
awk '/@TEST_DOCS@/ { system("cat test_docs.tmp"); next } 1' QUICKSTART.md.tmp > QUICKSTART.md
rm test_docs.tmp QUICKSTART.md.tmp

# Create version info file
cat > VERSION <<VERSION_EOF
VillageSQL Version: $VSQL_VERSION
Platform: $PLATFORM-$ARCH
Build Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Package Type: Development Server
VERSION_EOF

FILE_COUNT=$(find "$DEST_DIR" -type f | wc -l | tr -d ' ')
log_step "Dev server staged: $DEST_DIR ($FILE_COUNT files)"

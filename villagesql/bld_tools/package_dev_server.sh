#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Package a previously-built VillageSQL development server into a release
# tarball. This is the packaging part of the build -> test -> package pipeline;
# it assumes the server has already been built in BUILD_DIR (e.g. by
# villagesql/bld_tools/build_server.sh) and, when BUILD_BUNDLED_EXTENSIONS=1,
# that the bundled extension VEBs have already been built into
# $BUILD_DIR/veb_output_directory.
#
# The tree inside the tarball is assembled by stage_dev_server.sh; this script
# only names, tars, and delivers it. To install that tree into a directory
# without a tarball, use build_dev_server.sh or stage_dev_server.sh directly.
#
# Environment:
#   BUILD_DIR   Directory containing the completed build
#               (default: <repo-parent>/build)
#   OUTPUT_DIR  Where the final tarball is written (default: $PWD)
#   BUILD_BUNDLED_EXTENSIONS
#               "1" to fold bundled extension VEBs into the package

set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${SOURCE_DIR:-$(cd "$TOOLS_DIR/../.." && pwd)}"

BUILD_DIR="${BUILD_DIR:-$(cd "$SOURCE_DIR/.." && pwd)/build}"
OUTPUT_DIR="${OUTPUT_DIR:-$PWD}"
STAGING_DIR="${TMPDIR:-/tmp}/villagesql_dev_staging_$$"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

cleanup() {
    if [[ -d "$STAGING_DIR" ]]; then
        log_info "Cleaning up staging directory..."
        rm -rf "$STAGING_DIR"
    fi
    return 0
}

trap cleanup EXIT

source "$TOOLS_DIR/build_info.sh"
vsql_parse_version "$SOURCE_DIR"
vsql_platform_info

PACKAGE_NAME="villagesql-dev-server-${VSQL_CODE_BASE}_${VSQL_VERSION}-${PLATFORM}-${ARCH}"
TARBALL_NAME="${PACKAGE_NAME}.tar.gz"

log_info "Output Directory: $OUTPUT_DIR"
log_info "Package: $TARBALL_NAME"
echo ""

[[ -d "$BUILD_DIR" ]] || die "Build directory not found: $BUILD_DIR (build the server first)"

# The SDK is released as its own artifact, so the tarball never carries it.
SOURCE_DIR="$SOURCE_DIR" \
BUILD_DIR="$BUILD_DIR" \
BUILD_BUNDLED_EXTENSIONS="${BUILD_BUNDLED_EXTENSIONS:-0}" \
INCLUDE_SDK=0 \
    "$TOOLS_DIR/stage_dev_server.sh" "$STAGING_DIR/$PACKAGE_NAME"

log_step "Creating final tarball..."
cd "$STAGING_DIR"
tar czf "$TARBALL_NAME" "$PACKAGE_NAME"

# Move to output directory
mkdir -p "$OUTPUT_DIR"
mv "$TARBALL_NAME" "$OUTPUT_DIR/"

FINAL_SIZE=$(du -h "$OUTPUT_DIR/$TARBALL_NAME" | cut -f1)
FILE_COUNT=$(find "$PACKAGE_NAME" -type f | wc -l | tr -d ' ')

log_step "Package created successfully!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  📦 Package: $TARBALL_NAME"
echo "  📊 Size: $FINAL_SIZE"
echo "  📁 Files: $FILE_COUNT"
echo "  📍 Location: $OUTPUT_DIR/$TARBALL_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "To use:"
echo "  1. Extract: tar xzf $TARBALL_NAME"
echo "  2. cd $PACKAGE_NAME"
echo "  3. Read: cat QUICKSTART.md"
echo "  4. Initialize: ./villagesql init"
echo "  5. Start: ./villagesql start"
echo "  6. Connect: ./villagesql connect"
echo ""

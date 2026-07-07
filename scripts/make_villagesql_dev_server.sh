#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Script to create a standalone VillageSQL development server tarball

set -e  # Exit on error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TOOLS_DIR="$SOURCE_DIR/villagesql/bld_tools"
source "$SCRIPT_DIR/vsql_script_utils.sh"

BUILD_DIR="${BUILD_DIR:-$(cd "$SOURCE_DIR/.." && pwd)/build}"
OUTPUT_DIR="${OUTPUT_DIR:-$PWD}"

# _EXT_CLONES_DIR is set in step 2.5 if BUILD_BUNDLED_EXTENSIONS=1, so that
# the extension source clones survive long enough for test_extension_vebs.sh.
_EXT_CLONES_DIR=""

cleanup() {
    [[ -n "$_EXT_CLONES_DIR" ]] && rm -rf "$_EXT_CLONES_DIR"
    return 0
}

trap cleanup EXIT

log_step "VillageSQL Development Server Package Builder"
echo ""

vsql_parse_version "$SOURCE_DIR"
vsql_platform_info

log_info "VillageSQL Version: $VSQL_VERSION"
log_info "Platform: $PLATFORM-$ARCH"
log_info "Build Directory: $BUILD_DIR"
log_info "Output Directory: $OUTPUT_DIR"
if [[ "${BUILD_BUNDLED_EXTENSIONS:-0}" == "1" ]]; then
    log_info "With bundled extensions"
fi

log_step "Step 1: Configure and build..."
BUILD_DIR="$BUILD_DIR" SOURCE_DIR="$SOURCE_DIR" \
    "$TOOLS_DIR/build_ci.sh" || die "build_ci.sh failed"

if [[ "${BUILD_BUNDLED_EXTENSIONS:-0}" == "1" ]]; then
    log_step "Step 2: Building bundled extensions..."
    SDK_STAGING_DIR="$BUILD_DIR/villagesql-extension-sdk-${VSQL_VERSION}"
    [[ -d "$SDK_STAGING_DIR" ]] || die "SDK staging directory not found: $SDK_STAGING_DIR"

    # Keep sources in a temp dir so test_extension_vebs.sh can read mysql-test/
    # directories after build_bundled_extensions.sh exits.
    _EXT_CLONES_DIR="$(mktemp -d)"
    EXTENSION_CLONES_DIR="$_EXT_CLONES_DIR" \
        "$SOURCE_DIR/scripts/build_bundled_extensions.sh" \
            "$SDK_STAGING_DIR" \
            "$BUILD_DIR/veb_output_directory"
    log_info "Bundled extensions built"

    log_step "Step 2.5: Testing bundled extensions..."
    "$SOURCE_DIR/scripts/test_extension_vebs.sh" \
        "$BUILD_DIR" \
        "$_EXT_CLONES_DIR"
    log_info "Bundled extensions tested"
else
    log_info "Skipping bundled extensions (set BUILD_BUNDLED_EXTENSIONS=1 to include)"
fi

log_step "Step 3: Packaging dev server tarball..."
BUILD_DIR="$BUILD_DIR" OUTPUT_DIR="$OUTPUT_DIR" \
    BUILD_BUNDLED_EXTENSIONS="${BUILD_BUNDLED_EXTENSIONS:-0}" \
    "$TOOLS_DIR/package_dev_server.sh" \
    || die "package_dev_server.sh failed"

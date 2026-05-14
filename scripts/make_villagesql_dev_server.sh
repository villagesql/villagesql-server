#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Script to create a standalone VillageSQL development server tarball

set -e  # Exit on error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/vsql_script_utils.sh"
BUILD_DIR="${BUILD_DIR:-$(cd "$SOURCE_DIR/.." && pwd)/build}"
OUTPUT_DIR="${OUTPUT_DIR:-$PWD}"
STAGING_DIR="${TMPDIR:-/tmp}/villagesql_dev_staging_$$"

# _EXT_CLONES_DIR is set in step 2.5 if BUILD_BUNDLED_EXTENSIONS=1, so that
# the extension source clones survive long enough for test_extension_vebs.sh.
_EXT_CLONES_DIR=""

cleanup() {
    if [[ -d "$STAGING_DIR" ]]; then
        log_info "Cleaning up staging directory..."
        rm -rf "$STAGING_DIR"
    fi
    [[ -n "$_EXT_CLONES_DIR" ]] && rm -rf "$_EXT_CLONES_DIR"
    return 0
}

trap cleanup EXIT

log_step "VillageSQL Development Server Package Builder"
echo ""

# Verify source directory exists
if [[ ! -d "$SOURCE_DIR" ]]; then
    die "Source directory not found: $SOURCE_DIR"
fi

if [[ ! -f "$SOURCE_DIR/CMakeLists.txt" ]]; then
    die "Source directory doesn't appear to be valid (no CMakeLists.txt): $SOURCE_DIR"
fi

# Create build directory if it doesn't exist
mkdir -p "$BUILD_DIR"

vsql_parse_version "$SOURCE_DIR"
vsql_platform_info

PACKAGE_NAME="villagesql-dev-server-${VSQL_VERSION}-${PLATFORM}-${ARCH}"
TARBALL_NAME="${PACKAGE_NAME}.tar.gz"

log_info "VillageSQL Version: $VSQL_VERSION"
log_info "Platform: $PLATFORM-$ARCH"
log_info "Build Directory: $BUILD_DIR"
log_info "Output Directory: $OUTPUT_DIR"
log_info "Package includes:"
log_info "  - Server and client binaries"
log_info "  - Example VEB files (vsql_simple, vsql_complex)"
if [[ "${BUILD_BUNDLED_EXTENSIONS:-0}" == "1" ]]; then
    log_info "  - Bundled extensions (from villagesql/dev_server/bundled_extensions.txt)"
fi
log_info "  - mysql-test framework (binaries only, no test/result files)"
log_info "  - Support files and SQL scripts"
echo ""

log_step "Step 1: Configure and build..."
BUILD_DIR="$BUILD_DIR" SOURCE_DIR="$SOURCE_DIR" \
    "$SCRIPT_DIR/build-ci.sh" || die "build-ci.sh failed"

# TODO(villagesql): Extract package_dev_server.sh as a separate script so that
# CI can run build → test → package as discrete steps rather than
# embedding the test between two halves of this monolithic script.
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

# Create staging directory
mkdir -p "$STAGING_DIR"
cd "$BUILD_DIR"

log_step "Step 3: Generating base package with CPack..."

CPACK_COMPONENTS="Client;Server;Server_Scripts;SharedLibraries;SupportFiles;Readme;Info;ExampleVebs;Test;TestReadme"

log_info "CPack components: $CPACK_COMPONENTS"

# Run cpack with specific components
cpack -G TGZ \
    -D CPACK_ARCHIVE_COMPONENT_INSTALL=ON \
    -D CPACK_COMPONENTS_GROUPING=ALL_COMPONENTS_IN_ONE \
    -D "CPACK_COMPONENTS_ALL=${CPACK_COMPONENTS}" \
    -D CPACK_PACKAGE_FILE_NAME="villagesql-base-temp" \
    >/dev/null 2>&1 || die "CPack failed. Check that the build is complete."

BASE_TARBALL="villagesql-base-temp.tar.gz"
if [[ ! -f "$BASE_TARBALL" ]]; then
    die "CPack did not create expected tarball: $BASE_TARBALL"
fi

ORIGINAL_SIZE=$(du -h "$BASE_TARBALL" | cut -f1)
log_info "Base package size: $ORIGINAL_SIZE"

log_step "Step 4: Extracting base package..."
cd "$STAGING_DIR"

# CPack creates tarballs without a wrapping directory, so create one first
mkdir -p "$PACKAGE_NAME"
cd "$PACKAGE_NAME"
tar xzf "$BUILD_DIR/$BASE_TARBALL"

log_step "Step 4.5: Stripping unnecessary test data..."
if [[ -d "mysql-test" ]]; then
        log_info "Removing unnecessary test data from std_data..."
        if [[ -d "mysql-test/std_data" ]]; then
            # Keep only essential SSL certificate files that work together
            # Save matching SSL certificate/key pairs temporarily
            mkdir -p /tmp/mtr_ssl_$$

            # Copy only the standard CA and server/client cert pairs (not test fixtures)
            for file in cacert.pem \
                        server-cert.pem server-key.pem \
                        client-cert.pem client-key.pem; do
                if [[ -f "mysql-test/std_data/$file" ]]; then
                    cp "mysql-test/std_data/$file" /tmp/mtr_ssl_$$/ 2>/dev/null || true
                fi
            done

            # Remove std_data and recreate it
            rm -rf mysql-test/std_data
            mkdir -p mysql-test/std_data

            # Restore SSL certificates
            if [[ -d /tmp/mtr_ssl_$$ ]]; then
                mv /tmp/mtr_ssl_$$/* mysql-test/std_data/ 2>/dev/null || true
                rm -rf /tmp/mtr_ssl_$$
            fi

            STD_DATA_SIZE=$(du -sh mysql-test/std_data 2>/dev/null | cut -f1 || echo "unknown")
            log_info "Preserved essential SSL certificates in std_data ($STD_DATA_SIZE)"
        fi

        log_info "Removing all test suites and test/result files..."
        rm -rf mysql-test/suite mysql-test/r mysql-test/t

    # Calculate space saved
    MYSQL_TEST_SIZE=$(du -sh mysql-test 2>/dev/null | cut -f1 || echo "unknown")
    log_info "mysql-test framework size after cleanup: $MYSQL_TEST_SIZE"
fi

if [[ "${BUILD_BUNDLED_EXTENSIONS:-0}" == "1" ]]; then
    log_step "Step 5: Adding bundled extensions..."

    "$SOURCE_DIR/scripts/include_bundled_extensions.sh" \
            "lib/veb" \
            "$BUILD_DIR/veb_output_directory"

    log_info "Bundled extensions added to release"

else
    log_info "Step 6: Skipping bundled extensions (set BUILD_BUNDLED_EXTENSIONS=1 to include)"
fi

# Step 5: Add convenience scripts
log_step "Step 5: Adding convenience scripts..."

# Copy script from source directory
TEMPLATE_DIR="$SOURCE_DIR/villagesql/dev_server"

cp "$TEMPLATE_DIR/villagesql.sh" villagesql && chmod +x villagesql

# Verify script was copied
if [[ ! -f "villagesql" ]]; then
    die "Failed to copy villagesql from $TEMPLATE_DIR"
fi

log_info "Convenience scripts added"

# Step 6: Create comprehensive README
log_step "Step 6: Creating documentation..."

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
cat > VERSION <<EOF
VillageSQL Version: $VSQL_VERSION
Platform: $PLATFORM-$ARCH
Build Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Package Type: Development Server
EOF

# Step 7: Create the final tarball
log_step "Step 7: Creating final tarball..."
cd "$STAGING_DIR"
tar czf "$TARBALL_NAME" "$PACKAGE_NAME"

# Move to output directory
mkdir -p "$OUTPUT_DIR"
mv "$TARBALL_NAME" "$OUTPUT_DIR/"

# Get final size
FINAL_SIZE=$(du -h "$OUTPUT_DIR/$TARBALL_NAME" | cut -f1)

# Calculate number of files
FILE_COUNT=$(find "$PACKAGE_NAME" -type f | wc -l | tr -d ' ')

# Clean up
cd "$BUILD_DIR"
rm -f "villagesql-base-temp.tar.gz"

log_step "Package created successfully!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  📦 Package: $TARBALL_NAME"
echo "  📊 Size: $FINAL_SIZE (original CPack: $ORIGINAL_SIZE)"
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

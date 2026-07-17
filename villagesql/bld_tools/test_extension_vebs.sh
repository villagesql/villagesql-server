#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Run MTR test suites for bundled extensions against a VillageSQL build.
#
# Usage: test_extension_vebs.sh <build_dir> <extension_clones_dir>
#
# <build_dir>:   The VillageSQL build directory (output of build_ci.sh). mysqld must
#                be present at runtime_output_directory/mysqld within this directory.
# <extension_clones_dir>: Directory of cloned extension repos (one subdir per extension),
#                produced by build_bundled_extensions.sh with EXTENSION_CLONES_DIR
#                set. Extensions that contain a mysql-test/ directory have their
#                suites exercised.
#
# Extension test convention: each extension repo must have a mysql-test/ directory
# at its root, structured as a single MTR suite (t/ and r/ subdirectories). This
# directory is temporarily mounted as mysql-test/suite/<extension-name>/ in the
# source tree while tests run, then removed on exit.

set -e

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$TOOLS_DIR/../.." && pwd)"
source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

BUILD_DIR="${1:?Usage: $0 <build_dir> <extension_clones_dir>}"
EXTENSION_CLONES_DIR="${2:?Usage: $0 <build_dir> <extension_clones_dir>}"

MTR_SUITE_DIR="$SOURCE_DIR/mysql-test/suite"
MYSQLD="$BUILD_DIR/runtime_output_directory/mysqld"
MTR="$SOURCE_DIR/mysql-test/mysql-test-run.pl"

[[ -x "$MYSQLD" ]] || die "mysqld not found at $MYSQLD"
[[ -f "$MTR" ]]    || die "mysql-test-run.pl not found at $MTR"
[[ -d "$EXTENSION_CLONES_DIR" ]] || die "Sources directory not found: $EXTENSION_CLONES_DIR"

# Collect suites to run and stage them in the source tree temporarily.
# Cleanup removes only the directories we created.
STAGED=()
cleanup() {
    for name in "${STAGED[@]:-}"; do
        [[ -n "$name" ]] && rm -rf "$MTR_SUITE_DIR/$name"
    done
}
trap cleanup EXIT

SUITES=""
for ext_dir in "$EXTENSION_CLONES_DIR"/*/; do
    [[ -d "$ext_dir" ]] || continue
    name="$(basename "$ext_dir")"

    if [[ ! -d "$ext_dir/mysql-test" ]]; then
        log_warn "$name: no mysql-test/ directory, skipping tests"
        continue
    fi

    log_info "Staging test suite: $name"
    cp -r "$ext_dir/mysql-test" "$MTR_SUITE_DIR/$name"
    STAGED+=("$name")
    SUITES="${SUITES:+$SUITES,}$name"
done

if [[ -z "$SUITES" ]]; then
    log_info "No extension test suites found, skipping."
    exit 0
fi

log_step "Running extension MTR suites: $SUITES"
NCORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo "4")

# MTR must be invoked from its own directory; MTR_BINDIR tells it where to
# find the built mysqld and client binaries.
#
# --force: continue past individual test failures so all suites are exercised
# and all failures are visible in one run.
cd "$SOURCE_DIR/mysql-test"
MTR_EXIT=0
MTR_BINDIR="$BUILD_DIR" perl mysql-test-run.pl \
    --suite="$SUITES" \
    --nounit-tests \
    --parallel="$NCORES" \
    --force \
    --retry=0 \
    || MTR_EXIT=$?

if [[ $MTR_EXIT -ne 0 ]]; then
    log_error "Extension tests failed (see above for details)"
    exit $MTR_EXIT
fi

log_info "All extension tests passed"

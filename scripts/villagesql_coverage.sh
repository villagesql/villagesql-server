#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Collect VillageSQL coverage.
#
# By default it does NOT run the upstream MySQL regression: the delta is
# VillageSQL-authored code (new villagesql/* plus custom-type branches added to
# core files), which the in-tree villagesql/extension tests exercise.
#
# Usage: villagesql_coverage.sh <build_dir> [server_mtr_args...]
#
#   <build_dir>:       A gcov-instrumented build (configured with ENABLE_GCOV=1).
#                      mysqld must exist at runtime_output_directory/mysqld.
#   [server_mtr_args]: Optional. If given, ALSO runs the upstream MySQL
#                      regression with these args (e.g. "--suite=all") to
#                      exercise VillageSQL's edits to core files that the
#                      villagesql suite alone doesn't reach. Omit for a
#                      delta-focused run (villagesql suite + unit tests).
#
# Coverage accumulates into the build's .gcda files (the absolute paths baked
# into the instrumented mysqld), so every test phase below adds to one dataset
# between 'fastcov-clean' and 'fastcov-report'.
#
# Prerequisites:
#   - Tools: git, cmake, perl, python3, lcov (genhtml).
#   - Run as a NON-root user (mysqld refuses to run as root). That user needs
#     write access to the build dir (for .gcda) and the source tree's
#     mysql-test/ (mtr writes its var/ there).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SOURCE_DIR/villagesql/scripts/vsql_script_utils.sh"

BUILD_DIR="${1:?Usage: $0 <build_dir> [server_mtr_args...]}"
shift || true
SERVER_MTR_ARGS=("$@")

[[ -d "$BUILD_DIR" ]] || die "Build dir not found: $BUILD_DIR"
BUILD_DIR="$(cd "$BUILD_DIR" && pwd)"

# mysqld refuses to run as root; the whole flow must run unprivileged.
[[ "${EUID:-$(id -u)}" -ne 0 ]] || die "Do not run as root (mysqld refuses it). Run as the 'mtr' user."

MYSQLD="$BUILD_DIR/runtime_output_directory/mysqld"
MTR="$SOURCE_DIR/mysql-test/mysql-test-run.pl"

[[ -x "$MYSQLD" ]] || die "mysqld not found at $MYSQLD (is this a gcov build?)"
[[ -f "$MTR" ]]    || die "mysql-test-run.pl not found at $MTR"
for tool in git cmake perl python3 genhtml; do
    command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

# Test phases may fail individual tests without invalidating the coverage they
# produced, so run them soft: log, record, and continue rather than abort.
WARNINGS=()
soft() {
    local label="$1"; shift
    log_step "$label"
    local rc=0
    "$@" || rc=$?
    if (( rc != 0 )); then
        log_warn "$label exited rc=$rc; continuing (coverage still captured)"
        WARNINGS+=("$label (rc=$rc)")
    fi
}

# mtr must be invoked from its own dir; MTR_BINDIR points it at THIS build so the
# instrumented mysqld runs and .gcda lands in $BUILD_DIR.
run_mtr() {
    ( cd "$SOURCE_DIR/mysql-test" && MTR_BINDIR="$BUILD_DIR" perl mysql-test-run.pl "$@" )
}

# --- 1. Zero coverage counters ------------------------------------------------
log_step "Zeroing coverage counters (fastcov-clean)"
make -C "$BUILD_DIR" fastcov-clean || die "fastcov-clean failed"

# --- 2. Optional upstream MySQL regression (opt-in via server_mtr_args) -------
# Off by default: it adds little to the VillageSQL delta (see header) while
# costing hours. Provide server args to opt in. --max-test-fail=0 keeps it from
# aborting early when a few tests fail (the coverage they produced is still
# collected).
if (( ${#SERVER_MTR_ARGS[@]} )); then
    soft "Upstream MySQL regression (${SERVER_MTR_ARGS[*]})" run_mtr \
        --parallel=auto --force --max-test-fail=0 "${SERVER_MTR_ARGS[@]}"
else
    log_info "Skipping upstream MySQL regression (delta-focused run; pass server mtr args to include it)"
fi

# --- 3. In-tree villagesql suite + unit tests ---------------------------------
# The villagesql suite installs the in-tree (instrumented) test extensions, so
# this is what produces the villagesql/sdk (dev) coverage folded into the delta.
# --big-test includes the longer tests (e.g. villagesql/startup upgrade
# scenarios) that are otherwise skipped, so their code paths are covered too.
soft "In-tree villagesql suite" run_mtr \
    --do-suite=villagesql --nounit-tests --parallel=auto --force --big-test
soft "villagesql unit tests" ctest --test-dir "$BUILD_DIR" -L villagesql

# --- 4. Delta report ----------------------------------------------------------
# Only the delta report is generated. The full code_coverage/ report (a slow
# genhtml pass over the whole tree) isn't needed for the delta and isn't the
# goal here; run 'make fastcov-html' manually if you ever want it.
log_step "Generating delta coverage report"
make -C "$BUILD_DIR" fastcov-report || die "fastcov-report failed"
soft "fastcov-diff (delta report)" make -C "$BUILD_DIR" fastcov-diff

echo ""
log_step "Coverage run complete"
log_info "Delta report: $BUILD_DIR/coverage-delta/index.html"
if (( ${#WARNINGS[@]} )); then
    log_warn "Phases with non-zero exit (coverage still collected):"
    for w in "${WARNINGS[@]}"; do log_warn "  - $w"; done
fi

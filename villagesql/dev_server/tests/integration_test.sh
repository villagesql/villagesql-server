#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
# Integration test for the VillageSQL dev server control script.
#
# Usage: integration_test.sh <package-dir>
#
# <package-dir> is the root of an extracted villagesql-dev-server-* tarball,
# containing the villagesql script and the bin/, mysql-test/, etc. directories.
#
# The test spins up a real server in a temporary instance directory, exercises
# each sub-command, then shuts it down and cleans up.

PACKAGE_DIR="${1:?Usage: $0 <package-dir>}"
VSQL="$PACKAGE_DIR/villagesql"

if [[ ! -f "$VSQL" ]]; then
    echo "Error: villagesql script not found at $VSQL"
    exit 1
fi

INSTANCE_DIR="$(mktemp -d)"
SOCKET_INSTANCE_DIR=""  # set once second instance with custom socket is created below
SOCKET_DIR=""           # set once custom socket dir is created below
PASS=0
FAIL=0

cleanup() {
    "$VSQL" --dir "$INSTANCE_DIR" stop 2>/dev/null || true
    [[ -n "$SOCKET_INSTANCE_DIR" ]] && "$VSQL" --dir "$SOCKET_INSTANCE_DIR" --socket "$SOCKET_DIR/mysql.sock" stop 2>/dev/null || true
    rm -rf "$INSTANCE_DIR" "$SOCKET_INSTANCE_DIR" "$SOCKET_DIR"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

run_test() {
    local desc="$1"
    local expected="$2"  # "success" or "failure"
    shift 2

    if "$@" >/dev/null 2>&1; then
        actual="success"
    else
        actual="failure"
    fi

    if [[ "$actual" == "$expected" ]]; then
        echo "PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc (expected $expected, got $actual)"
        FAIL=$((FAIL + 1))
    fi
}

run_test_output() {
    local desc="$1"
    local pattern="$2"
    shift 2

    local output
    output=$("$@" 2>&1) || true
    if echo "$output" | grep -qF "$pattern"; then
        echo "PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc"
        echo "  Expected output containing: $pattern"
        echo "  Got: $output"
        FAIL=$((FAIL + 1))
    fi
}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

echo "Testing VillageSQL dev server"
echo "  Package directory: $PACKAGE_DIR"
echo "  Instance directory: $INSTANCE_DIR"
echo ""

# Help and error handling
run_test_output "help shows usage"      "Usage:"   "$VSQL" help
run_test_output "--help shows usage"    "Usage:"   "$VSQL" --help
run_test        "unknown command fails" failure    "$VSQL" unknown_command_xyz

# Pre-init state
run_test "status before init fails"  failure "$VSQL" --dir "$INSTANCE_DIR" status
run_test "start before init fails"   failure "$VSQL" --dir "$INSTANCE_DIR" start
run_test "stop before init fails"    failure "$VSQL" --dir "$INSTANCE_DIR" stop

# Initialization
run_test "init succeeds"      success "$VSQL" --dir "$INSTANCE_DIR" init
run_test "double init fails"  failure "$VSQL" --dir "$INSTANCE_DIR" init

# Post-init, pre-start state
run_test "status before start fails" failure "$VSQL" --dir "$INSTANCE_DIR" status

# Start
run_test "start succeeds"        success "$VSQL" --dir "$INSTANCE_DIR" start
run_test "double start fails"    failure "$VSQL" --dir "$INSTANCE_DIR" start

# Running state
run_test        "status while running succeeds"  success "$VSQL" --dir "$INSTANCE_DIR" status
run_test_output "connect can run SELECT 1"       "1"     "$VSQL" --dir "$INSTANCE_DIR" connect -e "SELECT 1"
run_test        "veb ls succeeds"                success "$VSQL" --dir "$INSTANCE_DIR" veb ls

# veb add/rm
TMPVEB="$(mktemp /tmp/test_XXXXXX.veb)"
run_test        "veb add succeeds"               success "$VSQL" --dir "$INSTANCE_DIR" veb add "$TMPVEB"
run_test_output "veb ls shows added extension"   "$(basename "$TMPVEB" .veb)" \
                "$VSQL" --dir "$INSTANCE_DIR" veb ls
run_test        "veb rm succeeds"                success "$VSQL" --dir "$INSTANCE_DIR" veb rm "$(basename "$TMPVEB" .veb)"
run_test        "veb rm missing fails"           failure "$VSQL" --dir "$INSTANCE_DIR" veb rm nonexistent_ext
rm -f "$TMPVEB"

# Stop
run_test "stop succeeds"          success "$VSQL" --dir "$INSTANCE_DIR" stop
run_test "status after stop fails" failure "$VSQL" --dir "$INSTANCE_DIR" status
run_test "double stop fails"       failure "$VSQL" --dir "$INSTANCE_DIR" stop

# Custom --socket
SOCKET_INSTANCE_DIR="$(mktemp -d)"
SOCKET_DIR="$(mktemp -d)"
CUSTOM_SOCKET="$SOCKET_DIR/mysql.sock"

run_test        "init succeeds with --socket"            success "$VSQL" --dir "$SOCKET_INSTANCE_DIR" --socket "$CUSTOM_SOCKET" init
run_test        "start succeeds with --socket"           success "$VSQL" --dir "$SOCKET_INSTANCE_DIR" --socket "$CUSTOM_SOCKET" start
run_test        "socket file created at custom path"     success test -S "$CUSTOM_SOCKET"
run_test        "no socket file under --dir"             failure test -e "$SOCKET_INSTANCE_DIR/mysql.sock"
run_test_output "status reports custom socket path" "$CUSTOM_SOCKET" \
                "$VSQL" --dir "$SOCKET_INSTANCE_DIR" --socket "$CUSTOM_SOCKET" status
run_test_output "connect over --socket can run SELECT 1" "1" \
                "$VSQL" --dir "$SOCKET_INSTANCE_DIR" --socket "$CUSTOM_SOCKET" connect -e "SELECT 1"
run_test        "stop succeeds with --socket"            success "$VSQL" --dir "$SOCKET_INSTANCE_DIR" --socket "$CUSTOM_SOCKET" stop
run_test        "socket file removed after stop"         failure test -e "$CUSTOM_SOCKET"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] || exit 1

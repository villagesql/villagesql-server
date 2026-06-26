#!/bin/bash
# Verify script for skip_result_files test case
# Verifies that .result files are NOT modified by villint (trailing whitespace preserved)

set -e

RESULT_FILE="mysql-test/r/test_skip_result.result"

# Check if file exists
if [ ! -f "$RESULT_FILE" ]; then
    echo "FAIL: $RESULT_FILE does not exist"
    exit 1
fi

# Check that trailing whitespace is PRESERVED (villint should NOT strip it)
# The file should still have trailing whitespace on lines 3 and 4
if ! grep -q '	$' "$RESULT_FILE"; then
    echo "FAIL: $RESULT_FILE had its trailing whitespace removed"
    echo "villint.sh should skip .result files"
    exit 1
fi

echo "PASS: $RESULT_FILE trailing whitespace was preserved (villint skipped it)"
exit 0

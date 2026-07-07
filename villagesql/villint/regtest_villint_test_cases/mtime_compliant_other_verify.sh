#!/bin/bash
# Verify script for mtime_compliant_other test case
# Checks that the file is properly formatted (no trailing whitespace, has EOF newline)

FILE="scripts/villint_mtime_test.py"

if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check for trailing whitespace
if grep -q '[[:space:]]$' "$FILE"; then
    echo "FAIL: $FILE has trailing whitespace"
    exit 1
fi

# Check for proper EOF newline
if [ -n "$(tail -c1 "$FILE")" ]; then
    echo "FAIL: $FILE does not end with newline"
    exit 1
fi

echo "PASS: $FILE is properly formatted"
exit 0

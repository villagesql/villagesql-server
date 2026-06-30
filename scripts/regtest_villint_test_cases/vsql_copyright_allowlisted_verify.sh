#!/bin/bash
# Verify script for vsql_copyright_allowlisted test case
# For allowlisted files, we just verify the file exists and was modified
# We don't require VillageSQL copyright for allowlisted third-party files

FILE="extra/libbacktrace/sha793921876c981/internal.h"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check if our test modification is present
if ! grep -q "villint test comment" "$FILE"; then
    echo "FAIL: $FILE was not modified by test"
    exit 1
fi

# File exists and was modified - that's all we need to verify
# (villint.sh will handle checking the allowlist)
echo "PASS: $FILE exists and was modified"
exit 0

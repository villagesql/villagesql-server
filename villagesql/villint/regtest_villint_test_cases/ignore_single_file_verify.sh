#!/bin/bash
# Verify script for ignore_single_file test case
# Checks that the file was NOT modified (remains without copyright because it's ignored)

FILE="sql/villint_ignore_test.cc"

if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check that file does NOT have VillageSQL copyright
# Since file is ignored, it should remain unchanged
if grep -q "VillageSQL Contributors" "$FILE"; then
    echo "FAIL: $FILE has copyright (but should be ignored)"
    exit 1
fi

echo "PASS: File correctly ignored (no copyright added)"
exit 0

#!/bin/bash
# Verify script for mtime_compliant_cpp test case
# Checks that the file is properly formatted

FILE="sql/villint_mtime_test.cc"

if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check for VillageSQL copyright
if ! grep -q "VillageSQL Contributors" "$FILE"; then
    echo "FAIL: $FILE missing VillageSQL copyright"
    exit 1
fi

# Check for proper EOF newline
if [ -n "$(tail -c1 "$FILE")" ]; then
    echo "FAIL: $FILE does not end with newline"
    exit 1
fi

echo "PASS: $FILE is properly formatted"
exit 0

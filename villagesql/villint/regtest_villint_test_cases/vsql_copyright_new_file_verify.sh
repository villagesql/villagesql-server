#!/bin/bash
# Verify script for vsql_copyright_new_file test case
# Checks that sql/villint_new_file.cc has VillageSQL copyright

FILE="sql/villint_new_file.cc"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check if file contains VillageSQL in the copyright
if grep -q "VillageSQL" "$FILE"; then
    echo "PASS: $FILE contains VillageSQL copyright"
    exit 0
else
    echo "FAIL: $FILE does not contain VillageSQL copyright"
    exit 1
fi

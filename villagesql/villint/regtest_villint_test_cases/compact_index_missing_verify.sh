#!/bin/bash
# Verify script for compact_index_missing test case
# Checks that no VSQL enum values are used in com_stat[] without
# sqlcom_compact_index(). This is UNFIXABLE — villint can detect it
# but cannot auto-fix it.

FILE="sql/test_compact_index.cc"

if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check for VSQL enum in com_stat without sqlcom_compact_index
if grep 'com_stat\[.*SQLCOM_INSTALL_EXTENSION' "$FILE" | grep -qv 'sqlcom_compact_index'; then
    echo "FAIL: $FILE uses SQLCOM_INSTALL_EXTENSION in com_stat without sqlcom_compact_index()"
    exit 1
fi

echo "PASS: No raw VSQL com_stat access found"
exit 0

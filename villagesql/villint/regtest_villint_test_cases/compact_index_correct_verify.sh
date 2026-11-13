#!/bin/bash
# Verify script for compact_index_correct test case
# A file using sqlcom_compact_index() should pass villint cleanly.

FILE="sql/test_compact_index_ok.cc"

if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check for VSQL enum in com_stat without sqlcom_compact_index
if grep 'com_stat\[.*SQLCOM_INSTALL_EXTENSION' "$FILE" | grep -qv 'sqlcom_compact_index'; then
    echo "FAIL: $FILE uses SQLCOM_INSTALL_EXTENSION in com_stat without sqlcom_compact_index()"
    exit 1
fi

echo "PASS: VSQL com_stat access uses sqlcom_compact_index correctly"
exit 0

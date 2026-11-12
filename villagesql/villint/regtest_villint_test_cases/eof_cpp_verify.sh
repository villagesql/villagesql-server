#!/bin/bash
# Verify script for eof_cpp test case
# Checks that sql/mysqld.cc ends with a newline

FILE="sql/mysqld.cc"

# Check if file ends with a newline
# tail -c1 returns the last byte of the file
# If it's empty (just a newline), the file ends properly
# If it's not empty, the file doesn't end with a newline

if [ -n "$(tail -c1 "$FILE")" ]; then
    echo "FAIL: $FILE does not end with a newline"
    exit 1
else
    echo "PASS: $FILE ends with a newline"
    exit 0
fi

#!/bin/bash
# Verify script for trailing_whitespace test case
# Checks that sql_yacc.yy has no trailing whitespace and preserves newlines

FILE="sql/sql_yacc.yy"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check for trailing whitespace (spaces, tabs, carriage returns at end of lines)
if grep -q '[ 	]$' "$FILE"; then
    echo "FAIL: $FILE has trailing whitespace"
    exit 1
fi

# Check that the file still has multiple lines (newlines preserved)
line_count=$(wc -l < "$FILE")
if [ "$line_count" -lt 100 ]; then
    echo "FAIL: $FILE has only $line_count lines (newlines may have been stripped)"
    exit 1
fi

echo "PASS: $FILE has no trailing whitespace and preserves newlines"
exit 0

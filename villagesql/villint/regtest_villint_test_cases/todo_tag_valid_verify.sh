#!/bin/bash
# Verify script for todo_tag_valid test case
# Checks that the file exists and has valid TODO tags
# This is a NO_VIOLATION test - villint should pass without changes

FILE="villagesql/test_todo_valid.cc"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Verify the file contains our expected valid tags
if ! grep -q "TODO(villagesql-ga):" "$FILE"; then
    echo "FAIL: $FILE missing expected TODO(villagesql-ga) tag"
    exit 1
fi

if ! grep -q "TODO(villagesql-performance):" "$FILE"; then
    echo "FAIL: $FILE missing expected TODO(villagesql-performance) tag"
    exit 1
fi

echo "PASS: $FILE contains valid TODO tags"
exit 0

#!/bin/bash
# Verify script for todo_tag_invalid test case
# Checks that the file does NOT contain invalid TODO tags
# (This is an UNFIXABLE test, so this verify should always fail since
#  villint.sh cannot automatically fix invalid TODO tags)

FILE="villagesql/test_todo_invalid.cc"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check if file contains the invalid tag
if grep -q "TODO(villagesql-gaaaa)" "$FILE"; then
    echo "FAIL: $FILE contains invalid TODO tag 'villagesql-gaaaa'"
    exit 1
fi

echo "PASS: $FILE does not contain invalid TODO tags"
exit 0

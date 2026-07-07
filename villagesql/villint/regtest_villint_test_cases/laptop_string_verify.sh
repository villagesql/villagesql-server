#!/bin/bash
# Verify script for laptop_string test case
# Checks that the file does NOT contain forbidden laptop strings
# (This is an UNFIXABLE test, so this verify should always fail since
#  villint.sh cannot automatically remove laptop strings)

FILE="mysql-test/suite/villagesql/select/t/laptop_string_test.test"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check if file contains the forbidden string
if grep -q "dbentley" "$FILE"; then
    echo "FAIL: $FILE contains forbidden laptop string 'dbentley'"
    exit 1
fi

echo "PASS: $FILE does not contain forbidden laptop strings"
exit 0

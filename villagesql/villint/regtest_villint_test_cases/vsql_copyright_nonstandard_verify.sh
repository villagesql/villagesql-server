#!/bin/bash
# Verify script for vsql_copyright_nonstandard test case
# Checks that the file has VillageSQL copyright
# (This is an UNFIXABLE test, so this verify should always fail since
#  villint.sh cannot fix non-standard copyrights automatically)

FILE="sql/villint_nonstandard_copyright.cc"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check if file has VillageSQL copyright
if grep -q "VillageSQL" "$FILE"; then
    echo "PASS: $FILE has VillageSQL copyright"
    exit 0
fi

echo "FAIL: $FILE does not have VillageSQL copyright"
exit 1

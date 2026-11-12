#!/bin/bash
# Verify script for vsql_copyright_indentation test case
# Checks that VillageSQL copyright line has same indentation as Oracle copyright

set -e

FILE="storage/innobase/test_indentation.cc"

# Check if file contains VillageSQL copyright
if ! grep -q "VillageSQL Contributors" "$FILE"; then
    echo "FAIL: $FILE does not contain VillageSQL copyright"
    exit 1
fi

# Extract the Oracle copyright line and the VillageSQL copyright line
ORACLE_LINE=$(grep "Copyright.*Oracle" "$FILE" | head -1)
VILLAGESQL_LINE=$(grep "VillageSQL Contributors" "$FILE")

# Extract the indentation (leading whitespace) from both lines
ORACLE_INDENT=$(echo "$ORACLE_LINE" | sed 's/Copyright.*//')
VILLAGESQL_INDENT=$(echo "$VILLAGESQL_LINE" | sed 's/Copyright.*//')

# Check if indentations match
if [ "$ORACLE_INDENT" = "$VILLAGESQL_INDENT" ]; then
    echo "PASS: $FILE has matching indentation between Oracle and VillageSQL copyrights"
    echo "  Oracle indent: '$ORACLE_INDENT'"
    echo "  VillageSQL indent: '$VILLAGESQL_INDENT'"
    exit 0
else
    echo "FAIL: $FILE has mismatched indentation between Oracle and VillageSQL copyrights"
    echo "  Oracle line: '$ORACLE_LINE'"
    echo "  VillageSQL line: '$VILLAGESQL_LINE'"
    echo "  Oracle indent: '$ORACLE_INDENT'"
    echo "  VillageSQL indent: '$VILLAGESQL_INDENT'"
    exit 1
fi

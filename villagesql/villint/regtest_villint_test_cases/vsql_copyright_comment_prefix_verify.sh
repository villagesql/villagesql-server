#!/bin/bash
# Verify script for vsql_copyright_comment_prefix test case
# Checks that VillageSQL copyright line has same prefix as Oracle copyright

set -e

FILE="test_comment_prefix.cc"

# Check if file contains VillageSQL copyright
if ! grep -q "VillageSQL Contributors" "$FILE"; then
    echo "FAIL: $FILE does not contain VillageSQL copyright"
    exit 1
fi

# Extract the Oracle copyright line and the VillageSQL copyright line
ORACLE_LINE=$(grep "Copyright.*Oracle" "$FILE" | head -1)
VILLAGESQL_LINE=$(grep "VillageSQL Contributors" "$FILE")

# Check if the formatting is correct for comment blocks
ORACLE_PREFIX=$(echo "$ORACLE_LINE" | sed 's/Copyright.*//')
VILLAGESQL_PREFIX=$(echo "$VILLAGESQL_LINE" | sed 's/Copyright.*//')

# Extract just the indentation (whitespace) from both lines
ORACLE_INDENT=$(echo "$ORACLE_LINE" | sed 's/[^ \t].*//')
VILLAGESQL_INDENT=$(echo "$VILLAGESQL_LINE" | sed 's/[^ \t].*//')

# For comment prefix format (/* Copyright), VillageSQL line should have proper comment continuation
if echo "$ORACLE_LINE" | grep -q "^[[:space:]]*\/\*[[:space:]]*Copyright"; then
    # Oracle line starts a comment block - VillageSQL should continue it properly
    if echo "$VILLAGESQL_LINE" | grep -q "^[[:space:]]*Copyright"; then
        echo "PASS: $FILE has proper comment formatting - Oracle starts block, VillageSQL continues"
        echo "  Oracle line: '$ORACLE_LINE'"
        echo "  VillageSQL line: '$VILLAGESQL_LINE'"
        exit 0
    else
        echo "FAIL: $FILE VillageSQL line has wrong comment format for block continuation"
        exit 1
    fi
else
    # Standard indentation check - prefixes should match
    if [ "$ORACLE_PREFIX" = "$VILLAGESQL_PREFIX" ]; then
        echo "PASS: $FILE has matching indentation between Oracle and VillageSQL copyrights"
        exit 0
    else
        echo "FAIL: $FILE has mismatched indentation between Oracle and VillageSQL copyrights"
        exit 1
    fi
fi

#!/bin/bash
# Edit script for trailing_whitespace test case
# Adds trailing whitespace to a .yy file (non-C/C++ file)

set -e

FILE="sql/sql_yacc.yy"

echo "Adding trailing whitespace to $FILE"

# Add trailing spaces to the first few lines
# We'll modify lines 2-4 to add trailing spaces
perl -i -pe 'if ($. >= 2 && $. <= 4) { s/$/   / }' "$FILE"

echo "Done: $FILE now has trailing whitespace on lines 2-4"

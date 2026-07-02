#!/bin/bash
# Edit script for skip_result_files test case
# Creates a .result file with trailing whitespace that should NOT be modified

set -e

# Create a fake .result file in mysql-test with trailing whitespace
RESULT_FILE="mysql-test/r/test_skip_result.result"

mkdir -p "$(dirname "$RESULT_FILE")"

# Create a .result file with intentional trailing tabs
# This simulates auto-generated test output where whitespace may be significant
# Using printf to ensure actual tab characters at end of lines
printf 'SELECT * FROM test_table;\t\n' > "$RESULT_FILE"
printf 'col1\tcol2\t\n' >> "$RESULT_FILE"
printf 'value1\tvalue2\t\n' >> "$RESULT_FILE"

echo "Created $RESULT_FILE with trailing whitespace"

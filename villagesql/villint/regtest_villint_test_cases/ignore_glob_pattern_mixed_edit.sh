#!/bin/bash
# Edit script for ignore_glob_pattern_mixed test case
# Creates generated files that should be ignored via glob pattern
# Also creates a regular file WITH a violation that should be fixed

set -e

FILE1="sql/test.generated.cc"
FILE2="storage/test.generated.cc"
FILE3="sql/regular_test.cc"

echo "Creating generated files: $FILE1, $FILE2"
echo "Creating regular file with violation: $FILE3"

cat > "$FILE1" << 'EOF'
int generated_code_1 = 0;

EOF

cat > "$FILE2" << 'EOF'
int generated_code_2 = 0;

EOF

cat > "$FILE3" << 'EOF'
int regular_code = 0;

EOF

git add "$FILE1" "$FILE2" "$FILE3"

echo "Done: Generated files and regular file with violation created"

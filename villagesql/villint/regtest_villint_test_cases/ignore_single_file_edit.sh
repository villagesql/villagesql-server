#!/bin/bash
# Edit script for ignore_single_file test case
# Creates a C++ file without proper copyright that should be ignored

set -e

FILE="sql/villint_ignore_test.cc"

echo "Creating file without copyright: $FILE"

cat > "$FILE" << 'EOF'
// This file intentionally lacks a proper copyright header
// and should be ignored by villint

int test_ignored_file = 0;
EOF

git add "$FILE"

echo "Done: File without copyright created (will be ignored)"

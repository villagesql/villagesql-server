#!/bin/bash
# Edit script for ignore_directory test case
# Creates files in a directory that should be entirely ignored

set -e

DIR="mysql-test/villint_test"
FILE1="$DIR/test1.cc"
FILE2="$DIR/subdir/test2.cc"

echo "Creating test directory and files: $DIR"

mkdir -p "$DIR/subdir"

cat > "$FILE1" << 'EOF'
// File without proper copyright in ignored directory
int test1 = 0;
EOF

cat > "$FILE2" << 'EOF'
// Another file without proper copyright in ignored subdirectory
int test2 = 0;
EOF

git add "$DIR"

echo "Done: Test directory created with non-compliant files"

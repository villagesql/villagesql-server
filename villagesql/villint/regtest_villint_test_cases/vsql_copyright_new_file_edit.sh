#!/bin/bash
# Edit script for vsql_copyright_new_file test case
# Creates a new C++ file without a copyright header

set -e

FILE="sql/villint_new_file.cc"

echo "Creating new file $FILE"

# Create the new file with test content
echo "static int i = 0; // villint test" > "$FILE"

# Add it to git so villint will process it
git add "$FILE"

echo "Done: $FILE created and added to git"

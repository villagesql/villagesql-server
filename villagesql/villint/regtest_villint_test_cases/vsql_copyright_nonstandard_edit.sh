#!/bin/bash
# Edit script for vsql_copyright_nonstandard test case
# Creates a new file with a non-standard copyright

set -e

FILE="sql/villint_nonstandard_copyright.cc"

echo "Creating file with non-standard copyright: $FILE"

# Create a file with a non-standard copyright
cat > "$FILE" << 'EOF'
// Copyright (c) 2023 Some Random Person

static int i = 0; // villint test
EOF

# Add it to git so villint will process it
git add "$FILE"

echo "Done: $FILE created with non-standard copyright"

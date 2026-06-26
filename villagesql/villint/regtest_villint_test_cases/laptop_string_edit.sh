#!/bin/bash
# Edit script for laptop_string test case
# Creates a VillageSQL test file with a forbidden laptop string

set -e

# Use a directory that exists on origin/main (select/ is a stable subdirectory)
FILE="mysql-test/suite/villagesql/select/t/laptop_string_test.test"

echo "Creating test file with laptop string: $FILE"

# Create parent directory if needed (defensive)
mkdir -p "$(dirname "$FILE")"

# Create a test file with a laptop-specific path
cat > "$FILE" << 'EOF'
# Test case that accidentally contains a laptop string
# This should be caught by villint.sh

--echo Testing with path /home/dbentley/build
SELECT 1;
EOF

# Add it to git so villint will process it
git add "$FILE"

echo "Done: $FILE created with forbidden laptop string"

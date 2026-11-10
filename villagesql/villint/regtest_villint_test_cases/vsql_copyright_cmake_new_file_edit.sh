#!/bin/bash
# Edit script for vsql_copyright_cmake_new_file test case
# Creates a new CMakeLists.txt file without a copyright header

set -e

DIR="villagesql/test_cmake_dir"
FILE="$DIR/CMakeLists.txt"

echo "Creating directory $DIR"
mkdir -p "$DIR"

echo "Creating new file $FILE"

# Create the new file with test content
cat > "$FILE" << 'EOF'
ADD_LIBRARY(villint_test_lib STATIC
  test.cc)

TARGET_LINK_LIBRARIES(villint_test_lib PRIVATE something)
EOF

# Add it to git so villint will process it
git add "$FILE"

echo "Done: $FILE created and added to git"

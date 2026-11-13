#!/bin/bash
# Edit script for vsql_copyright_cmake_existing_file test case
# Adds a line to packaging/deb-in/CMakeLists.txt to make it a changed file

set -e

FILE="packaging/deb-in/CMakeLists.txt"

echo "Adding test line to $FILE"

# Add a test line at the end
echo "# villint testing comment" >> "$FILE"

echo "Done: $FILE modified with test line"

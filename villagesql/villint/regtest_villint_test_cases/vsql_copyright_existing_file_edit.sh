#!/bin/bash
# Edit script for vsql_copyright_existing_file test case
# Adds a line to rest_router_status.cc to make it a changed file

set -e

FILE="router/src/rest_router/src/rest_router_status.cc"

echo "Adding test line to $FILE"

# Add a test line at the end
echo "static int i = 0; // villint testing line" >> "$FILE"

echo "Done: $FILE modified with test line"

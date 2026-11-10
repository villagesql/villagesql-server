#!/bin/bash
# Verify script for vsql_copyright_cmake_existing_file test case
# Checks that packaging/deb-in/CMakeLists.txt has VillageSQL copyright

FILE="packaging/deb-in/CMakeLists.txt"

# Check if file contains VillageSQL in the copyright
if grep -q "VillageSQL Contributors" "$FILE"; then
    echo "PASS: $FILE contains VillageSQL copyright"
    exit 0
else
    echo "FAIL: $FILE does not contain VillageSQL copyright"
    exit 1
fi

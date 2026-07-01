#!/bin/bash
# Verify script for vsql_copyright_cmake_new_file test case
# Checks that villagesql/test_cmake_dir/CMakeLists.txt has VillageSQL copyright

FILE="villagesql/test_cmake_dir/CMakeLists.txt"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Check if file contains VillageSQL in the copyright
if grep -q "VillageSQL Contributors" "$FILE"; then
    echo "PASS: $FILE contains VillageSQL copyright"
    exit 0
else
    echo "FAIL: $FILE does not contain VillageSQL copyright"
    exit 1
fi

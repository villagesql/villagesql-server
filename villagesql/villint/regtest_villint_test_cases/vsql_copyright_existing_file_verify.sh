#!/bin/bash
# Verify script for vsql_copyright_existing_file test case
# Checks that router/src/rest_router/src/rest_router_status.cc has VillageSQL copyright

FILE="router/src/rest_router/src/rest_router_status.cc"

# Check if file contains VillageSQL in the copyright
if grep -q "VillageSQL" "$FILE"; then
    echo "PASS: $FILE contains VillageSQL copyright"
    exit 0
else
    echo "FAIL: $FILE does not contain VillageSQL copyright"
    exit 1
fi

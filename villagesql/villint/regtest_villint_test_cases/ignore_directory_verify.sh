#!/bin/bash
# Verify script for ignore_directory test case
# Checks that files were NOT modified (remain without copyright because directory is ignored)

FILE1="mysql-test/villint_test/test1.cc"
FILE2="mysql-test/villint_test/subdir/test2.cc"

for FILE in "$FILE1" "$FILE2"; do
    if [ ! -f "$FILE" ]; then
        echo "FAIL: $FILE does not exist"
        exit 1
    fi

    # Check that files do NOT have VillageSQL copyright
    # Since directory is ignored, files should remain unchanged
    if grep -q "VillageSQL Contributors" "$FILE"; then
        echo "FAIL: $FILE has copyright (but directory should be ignored)"
        exit 1
    fi
done

echo "PASS: All files correctly ignored (no copyright added)"
exit 0

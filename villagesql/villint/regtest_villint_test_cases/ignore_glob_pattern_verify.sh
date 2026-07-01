#!/bin/bash
# Verify script for ignore_glob_pattern test case
# Checks that generated files were NOT modified (remain without copyright because they match ignore pattern)
# Also checks that regular file WAS modified (got copyright added)

FILE1="sql/test.generated.cc"
FILE2="storage/test.generated.cc"
FILE3="sql/regular_test.cc"

# Check generated files were ignored
for FILE in "$FILE1" "$FILE2"; do
    if [ ! -f "$FILE" ]; then
        echo "FAIL: $FILE does not exist"
        exit 1
    fi

    # Check that files do NOT have VillageSQL copyright
    # Since files match the ignore pattern, they should remain unchanged
    if grep -q "VillageSQL Contributors" "$FILE"; then
        echo "FAIL: $FILE has copyright (but should be ignored by pattern)"
        exit 1
    fi
done

# Check regular file was processed
if [ ! -f "$FILE3" ]; then
    echo "FAIL: $FILE3 does not exist"
    exit 1
fi

# Regular file should have copyright added
if ! grep -q "VillageSQL Contributors" "$FILE3"; then
    echo "FAIL: $FILE3 missing copyright (should be processed normally)"
    exit 1
fi

echo "PASS: Generated files correctly ignored, regular file correctly processed"
exit 0

#!/bin/bash
# Verify script for rapidjson_include_order test case
# Checks that my_rapidjson_size_t.h comes before other rapidjson includes

FILE="sql/villint_rapidjson_test.h"

if [ ! -f "$FILE" ]; then
    echo "FAIL: $FILE does not exist"
    exit 1
fi

# Extract the include lines
INCLUDES=$(grep '^#include' "$FILE" || true)

if [ -z "$INCLUDES" ]; then
    echo "FAIL: No includes found in $FILE"
    exit 1
fi

# Find the line numbers of the includes
MY_RAPIDJSON_LINE=$(echo "$INCLUDES" | grep -n 'my_rapidjson_size_t.h' | cut -d: -f1 || true)
RAPIDJSON_DOC_LINE=$(echo "$INCLUDES" | grep -n 'rapidjson/document.h' | cut -d: -f1 || true)
RAPIDJSON_WRITER_LINE=$(echo "$INCLUDES" | grep -n 'rapidjson/prettywriter.h' | cut -d: -f1 || true)

# Check that all includes are present
if [ -z "$MY_RAPIDJSON_LINE" ]; then
    echo "FAIL: my_rapidjson_size_t.h not found in $FILE"
    exit 1
fi

if [ -z "$RAPIDJSON_DOC_LINE" ]; then
    echo "FAIL: rapidjson/document.h not found in $FILE"
    exit 1
fi

if [ -z "$RAPIDJSON_WRITER_LINE" ]; then
    echo "FAIL: rapidjson/prettywriter.h not found in $FILE"
    exit 1
fi

# Check if my_rapidjson_size_t.h comes before the other rapidjson includes
if [ "$MY_RAPIDJSON_LINE" -lt "$RAPIDJSON_DOC_LINE" ] && [ "$MY_RAPIDJSON_LINE" -lt "$RAPIDJSON_WRITER_LINE" ]; then
    echo "PASS: my_rapidjson_size_t.h is correctly ordered before other rapidjson includes"
    exit 0
else
    echo "FAIL: Include order is incorrect"
    echo "  my_rapidjson_size_t.h at position: $MY_RAPIDJSON_LINE"
    echo "  rapidjson/document.h at position: $RAPIDJSON_DOC_LINE"
    echo "  rapidjson/prettywriter.h at position: $RAPIDJSON_WRITER_LINE"
    echo ""
    echo "Current include order:"
    echo "$INCLUDES"
    exit 1
fi

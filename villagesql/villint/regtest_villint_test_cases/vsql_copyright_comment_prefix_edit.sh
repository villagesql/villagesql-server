#!/bin/bash
# Edit script for vsql_copyright_comment_prefix test case
# Creates a file with comment-prefixed Oracle copyright (/* Copyright format)

set -e

FILE="test_comment_prefix.cc"

echo "Creating $FILE with comment-prefixed Oracle copyright"

# Create a test file with comment prefix before copyright
cat > "$FILE" << 'EOF'
/* Copyright (c) 2016, 2025, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 */

#include "test.h"

// Test function
int test_function() {
    return 0;
}
EOF

echo "Done: $FILE created with comment-prefixed Oracle copyright"

#!/bin/bash
# Edit script for vsql_copyright_indented test case
# Creates a file with indented Oracle copyright to test indentation alignment

set -e

FILE="test_indented_copyright.cc"

echo "Creating $FILE with indented Oracle copyright"

# Create a test file with indented Oracle copyright
cat > "$FILE" << 'EOF'
/*
   Copyright (c) 2020, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License, version 2.0, as published by the
   Free Software Foundation.
*/

#include "test.h"

// Test function with indented copyright
int test_function() {
    return 0;
}
EOF

echo "Done: $FILE created with indented Oracle copyright"

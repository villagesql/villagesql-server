#!/bin/bash
# Edit script for vsql_copyright_indentation test case
# Creates a file with indented Oracle copyright to test indentation alignment

set -e

FILE="storage/innobase/test_indentation.cc"

echo "Creating $FILE with indented Oracle copyright"

# Create a test file with indented Oracle copyright (like some InnoDB files)
cat > "$FILE" << 'EOF'
/*****************************************************************************

Copyright (c) 1996, 2025, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

*****************************************************************************/

#include "test.h"

// Test function
int test_function() {
    return 0;
}
EOF

echo "Done: $FILE created with non-indented Oracle copyright"

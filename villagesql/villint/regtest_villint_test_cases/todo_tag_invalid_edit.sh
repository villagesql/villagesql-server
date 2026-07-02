#!/bin/bash
# Edit script for todo_tag_invalid test case
# Creates a C++ file with an invalid TODO tag

set -e

FILE="villagesql/test_todo_invalid.cc"

echo "Creating C++ file with invalid TODO tag: $FILE"

cat > "$FILE" << 'EOF'
/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 */

// TODO(villagesql-gaaaa): This is a typo - should be villagesql-ga
void test_function() {}
EOF

git add "$FILE"

echo "Done: $FILE created with invalid TODO tag 'villagesql-intial'"

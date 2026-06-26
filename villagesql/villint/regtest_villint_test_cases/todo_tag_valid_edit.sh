#!/bin/bash
# Edit script for todo_tag_valid test case
# Creates a C++ file with valid TODO tags - should pass villint

set -e

FILE="villagesql/test_todo_valid.cc"

echo "Creating C++ file with valid TODO tags: $FILE"

cat > "$FILE" << 'EOF'
/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 */

// TODO(villagesql-ga): Must handle before initial release
// TODO(villagesql-performance): Performance optimization
// TODO(villagesql-rebase): Check during MySQL rebases
// TODO(villagesql-windows): Work to support Windows
// TODO(villagesql-beta): Beta milestone
void test_function() {}
EOF

git add "$FILE"

echo "Done: $FILE created with valid TODO tags"

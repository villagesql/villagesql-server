#!/bin/bash
# Edit script for ignore_glob_pattern test case
# Creates generated files that should be ignored via glob pattern
# Also creates a regular file that should be processed normally

set -e

FILE1="sql/test.generated.cc"
FILE2="storage/test.generated.cc"
FILE3="sql/regular_test.cc"

echo "Creating generated files: $FILE1, $FILE2"
echo "Creating regular file: $FILE3"

cat > "$FILE1" << 'EOF'
int generated_code_1 = 0;
EOF

cat > "$FILE2" << 'EOF'
int generated_code_2 = 0;
EOF

cat > "$FILE3" << 'EOF'
/*
  Copyright (c) 2000, 2025, Oracle and/or its affiliates. All rights reserved.
  Copyright (c) 2026 VillageSQL Contributors

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is designed to work with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have either included with
  the program or referenced in the documentation.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

// Regular file - should be processed normally (already has copyright)
int regular_code = 0;
EOF

git add "$FILE1" "$FILE2" "$FILE3"

echo "Done: Generated files and regular file created"

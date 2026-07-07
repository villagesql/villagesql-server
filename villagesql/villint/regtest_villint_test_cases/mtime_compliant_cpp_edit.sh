#!/bin/bash
# Edit script for mtime_compliant_cpp test case
# Creates a properly formatted C++ file that villint should not modify

set -e

FILE="sql/villint_mtime_test.cc"

echo "Creating compliant C++ file: $FILE"

cat > "$FILE" << 'EOF'
/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

int properly_formatted = 0;
EOF

git add "$FILE"

echo "Done: Compliant C++ file created"

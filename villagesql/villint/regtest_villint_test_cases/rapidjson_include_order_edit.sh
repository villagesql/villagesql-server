#!/bin/bash
# Edit script for rapidjson_include_order test case
# Creates a header file with my_rapidjson_size_t.h in its own block (separated by blank line)
# This uses IncludeBlocks: Preserve to keep the correct ordering
# Villint should NOT change this file

set -e

FILE="sql/villint_rapidjson_test.h"

echo "Creating header file with correct rapidjson include block order: $FILE"

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

#ifndef SQL_VILLINT_RAPIDJSON_TEST_H
#define SQL_VILLINT_RAPIDJSON_TEST_H

#include "my_rapidjson_size_t.h"  // IWYU pragma: keep

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>

void test_function();

#endif  // SQL_VILLINT_RAPIDJSON_TEST_H
EOF

git add "$FILE"

echo "Done: Header file with correct rapidjson include block order created"

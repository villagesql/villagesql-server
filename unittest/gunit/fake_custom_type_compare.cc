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

#include <cstring>

#include "my_inttypes.h"

// Fake villagesql::CustomMemCompare for the gunit_small unit tests.
//
// The real implementation lives in villagesql/ and pulls in the full
// Item / TypeContext machinery, which the lightweight gunit_small tests
// deliberately do not link (they link only fakes plus sqlgunitlib). sql/
// filesort_utils.cc, compiled into sqlgunitlib, references this symbol through
// sql/cmp_varlen_keys.h, so a definition must exist in the gunit_small closure.
//
// gunit_small tests never construct a custom TypeContext, so the real function
// would always take its memcmp fallback path. This fake reproduces exactly that
// path: it does NOT apply the reverse flag, because for plain memcmp the sort
// direction is already encoded in the key layout (see util.h). Tests that need
// real custom-type comparison (unittest/gunit/villagesql) link
// server_unittest_library instead and get the real implementation.
//
// This mirrors the fake_costmodel.cc / fake_table.cc pattern, but with a live
// body rather than assert(false): unlike those link-only stubs, this symbol is
// actually called during sort comparisons.

class Item;

namespace villagesql {

int CustomMemCompare(const Item *, const uchar *data1, size_t,
                     const uchar *data2, size_t, size_t min_len, bool) {
  return memcmp(data1, data2, min_len);
}

}  // namespace villagesql

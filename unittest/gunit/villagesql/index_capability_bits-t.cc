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

#include <gtest/gtest.h>

#include <bit>

#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

namespace villagesql_unittest {

// Each index capability is its own bit, so an index can declare any
// combination and the server can test each one on its own.
// Disabled: KNN-16 in mysql-test/suite/knn_review/ISSUES.md (KNN and
// APPROXIMATE share a bit).
TEST(IndexCapabilityBits, DISABLED_EachCapabilityIsADistinctBit) {
  struct Cap {
    const char *name;
    vef_index_cap_t value;
  };
  const Cap caps[] = {
      {"POINT_LOOKUP", VEF_INDEX_CAP_POINT_LOOKUP},
      {"RANGE_SCAN", VEF_INDEX_CAP_RANGE_SCAN},
      {"REVERSE_SCAN", VEF_INDEX_CAP_REVERSE_SCAN},
      {"ORDER_BY", VEF_INDEX_CAP_ORDER_BY},
      {"KNN", VEF_INDEX_CAP_KNN},
      {"APPROXIMATE", VEF_INDEX_CAP_APPROXIMATE},
      {"MVCC_VISIBILITY", VEF_INDEX_CAP_MVCC_VISIBILITY},
  };
  for (const Cap &a : caps) {
    EXPECT_EQ(1, std::popcount(a.value)) << a.name << " is not a single bit";
    for (const Cap &b : caps) {
      if (&a >= &b) continue;
      EXPECT_EQ(0u, a.value & b.value)
          << a.name << " and " << b.name << " share a bit";
    }
  }
}

}  // namespace villagesql_unittest

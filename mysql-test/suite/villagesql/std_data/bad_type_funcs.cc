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

// Test UDF for bad type tests.
// Provides functions for testing type validation scenarios.

#include <villagesql/vsql.h>

#include <cstring>
#include <string_view>

// Generic FROM_STRING function
void f1_impl(std::string_view from, vsql::CustomResult out) {
  (void)from;

  auto buf = out.buffer();
  if (buf.size() < 16) return;  // wrapper default warning

  memset(buf.data(), 0, 16);
  out.set_length(16);
}

// Generic TO_STRING function
void f2_impl(vsql::CustomArg in, vsql::StringResult out) {
  (void)in;
  out.set("val");
}

// Generic COMPARE function
int f3_impl(vsql::CustomArg a, vsql::CustomArg b) {
  return memcmp(a.value().data(), b.value().data(), 16);
}

static constexpr const char kTestBadTypeName[] = "TESTBADTYPE";

constexpr auto TESTBADTYPE = vsql::make_type<kTestBadTypeName>()
                                 .persisted_length(16)
                                 .max_decode_buffer_length(64)
                                 .from_string<&f1_impl>()
                                 .to_string<&f2_impl>()
                                 .compare<&f3_impl>()
                                 .build();

// Register a simple type for testing - tests can use this extension
// to verify basic type operations work
VEF_GENERATE_ENTRY_POINTS(make_extension().type(TESTBADTYPE))

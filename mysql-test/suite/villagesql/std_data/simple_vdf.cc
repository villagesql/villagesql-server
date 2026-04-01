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

// Simple test UDFs for extension testing.
// Provides basic integer and string returning functions.

#include <villagesql/vsql.h>

#include <cstring>

using namespace vsql;

// Returns a constant integer value (42)
void simple_int_func_impl(IntResult out) { out.set(42); }

// Returns a constant string value ("Hello from UDF")
void simple_string_func_impl(StringResult out) {
  const char *msg = "Hello from UDF";
  size_t len = strlen(msg);
  auto buf = out.buffer();
  memcpy(buf.data(), msg, len);
  out.set_length(len);
}

// Returns the length of the input string
void simple_test_impl(StringArg input, IntResult out) {
  out.set(input.is_null() ? 0 : static_cast<long long>(input.value().size()));
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest")
        .func(make_func<&simple_int_func_impl>("simple_int_func")
                  .returns(INT)
                  .build())
        .func(make_func<&simple_string_func_impl>("simple_string_func")
                  .returns(STRING)
                  .buffer_size(100)
                  .build())
        .func(make_func<&simple_test_impl>("simple_test")
                  .returns(INT)
                  .param(STRING)
                  .build()))

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

// Negative-compile fixture: max_result_length() on a non-STRING return type.
//
// max_result_length() only sizes a STRING result column, so the typed function
// builder rejects it for any other return type. This extension declares
// .returns(INT) and then .max_result_length(1000); it MUST fail to compile.
// The paired test asserts the SDK build is rejected and names the trap
// max_result_length_is_only_valid_for_a_STRING_return_type.

#include <villagesql/vsql.h>

using namespace vsql;

void bad_impl(IntArg n, IntResult out) {
  if (n.is_null()) {
    out.set_null();
    return;
  }
  out.set(n.value());
}

VEF_GENERATE_ENTRY_POINTS(make_extension().func(make_func<&bad_impl>("bad")
                                                    .returns(INT)
                                                    .param(INT)
                                                    .max_result_length(1000)
                                                    .build()))

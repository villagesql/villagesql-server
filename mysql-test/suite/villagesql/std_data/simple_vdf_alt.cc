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

// Alternative simple test UDFs for extension testing.
// Used to test ambiguous function name resolution when multiple extensions
// provide the same function name.

#include <villagesql/extension.h>

#include <cstring>

// Returns a different constant integer value (99)
void simple_int_func_impl(vef_context_t *ctx, vef_vdf_result_t *out) {
  out->int_value = 99;
  out->type = VEF_RESULT_VALUE;
}

// Returns a different constant string value
void alt_string_func_impl(vef_context_t *ctx, vef_vdf_result_t *out) {
  const char *msg = "Hello from alt extension";
  size_t len = strlen(msg);
  memcpy(out->str_buf, msg, len);
  out->actual_len = len;
  out->type = VEF_RESULT_VALUE;
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension("simple_udf_alt", "0.0.1-devtest")
        .func(make_func<&simple_int_func_impl>("simple_int_func")
                  .returns(INT)
                  .build())
        .func(make_func<&alt_string_func_impl>("alt_string_func")
                  .returns(STRING)
                  .buffer_size(100)
                  .build()))

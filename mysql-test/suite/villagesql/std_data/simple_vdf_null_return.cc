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

// Test VDF that returns NULL based on runtime value, not argument nullability.
// Used to verify the IS NULL optimizer fold fix: a VDF called with constant
// non-nullable arguments must still be evaluated at runtime for IS NULL.

#include <villagesql/extension.h>

#include <cstring>

// Returns NULL if input is "invalid", otherwise echoes the string back.
void echo_or_null_impl(vef_context_t *ctx, vef_invalue_t *input,
                       vef_vdf_result_t *out) {
  if (input->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  if (strncmp(input->str_value, "invalid", input->str_len) == 0) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  memcpy(out->str_buf, input->str_value, input->str_len);
  out->actual_len = input->str_len;
  out->type = VEF_RESULT_VALUE;
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest")
        .func(make_func<&echo_or_null_impl>("echo_or_null")
                  .returns(STRING)
                  .param(STRING)
                  .buffer_size(256)
                  .build()))

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

// Test extension with one deterministic VDF and one non-deterministic VDF.
// Used to verify that only deterministic VDFs are allowed in CHECK constraints.

#include <villagesql/extension.h>

// Returns val + 1. Declared deterministic.
void deterministic_inc_impl(vef_context_t *ctx, vef_invalue_t *val,
                            vef_vdf_result_t *out) {
  if (val->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  out->int_value = val->int_value + 1;
  out->type = VEF_RESULT_VALUE;
}

// Returns val * 2. Not declared deterministic.
void nondeterministic_func_impl(vef_context_t *ctx, vef_invalue_t *val,
                                vef_vdf_result_t *out) {
  if (val->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  out->int_value = val->int_value * 2;
  out->type = VEF_RESULT_VALUE;
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "1.0.0")
        .func(make_func<&deterministic_inc_impl>("deterministic_inc")
                  .returns(INT)
                  .param(INT)
                  .deterministic()
                  .build())
        .func(make_func<&nondeterministic_func_impl>("nondeterministic_func")
                  .returns(INT)
                  .param(INT)
                  .build()))

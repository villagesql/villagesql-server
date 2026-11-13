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

// My VillageSQL Extension
//
// This is a template for creating VillageSQL extensions.
// See the extension.h header for full documentation.

#include <villagesql/extension.h>

// Example function: adds two integers
// INT parameters are passed natively in the int_value field.
void add_impl(vef_context_t *ctx, vef_invalue_t *a, vef_invalue_t *b,
              vef_vdf_result_t *result) {
  if (a->is_null || b->is_null) {
    result->type = VEF_RESULT_NULL;
    return;
  }

  result->int_value = a->int_value + b->int_value;
  result->type = VEF_RESULT_VALUE;
}

// Register the extension
VEF_GENERATE_ENTRY_POINTS(make_extension("my_extension", "0.0.1")
                              .func(make_func<&add_impl>("my_add")
                                        .returns(INT)
                                        .param(INT)
                                        .param(INT)
                                        .build()))

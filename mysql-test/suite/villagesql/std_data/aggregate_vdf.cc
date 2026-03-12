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

// Test aggregate VDFs for VillageSQL.
// Provides vdf_sum(INT) -> INT and vdf_count(INT) -> INT aggregate functions,
// plus a scalar function simple_double(INT) -> INT to test mixed usage.

#include <villagesql/extension.h>

#include <cstring>

// =============================================================================
// Accumulator state, allocated in prerun, freed in postrun.
// =============================================================================

struct SumState {
  long long total;
  bool has_value;
};

struct CountState {
  long long count;
};

// =============================================================================
// vdf_sum: aggregate that sums INT values, returns NULL for empty groups
// =============================================================================

void vdf_sum_prerun(vef_context_t *ctx, vef_prerun_args_t *args,
                    vef_prerun_result_t *result) {
  auto *state = new SumState{0, false};
  result->user_data = state;
  result->type = VEF_RESULT_VALUE;
}

void vdf_sum_postrun(vef_context_t *ctx, vef_postrun_args_t *args,
                     vef_postrun_result_t *result) {
  delete static_cast<SumState *>(args->user_data);
}

void vdf_sum_clear(vef_context_t *ctx, vef_vdf_args_t *args) {
  auto *state = static_cast<SumState *>(args->user_data);
  state->total = 0;
  state->has_value = false;
}

void vdf_sum_add(vef_context_t *ctx, vef_vdf_args_t *args,
                 vef_vdf_result_t *result) {
  auto *state = static_cast<SumState *>(args->user_data);
  vef_invalue_t val = villagesql::func_builder::get_invalue(ctx, args, 0);
  if (!val.is_null) {
    state->total += val.int_value;
    state->has_value = true;
  }
}

// Aggregate result function with standard VDF signature
void vdf_sum_result_impl(vef_context_t *ctx, vef_vdf_args_t *args,
                         vef_vdf_result_t *out) {
  auto *state = static_cast<SumState *>(args->user_data);
  if (!state->has_value) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  out->int_value = state->total;
  out->type = VEF_RESULT_VALUE;
}

// =============================================================================
// vdf_count: aggregate that counts non-NULL INT values
// =============================================================================

void vdf_count_prerun(vef_context_t *ctx, vef_prerun_args_t *args,
                      vef_prerun_result_t *result) {
  auto *state = new CountState{0};
  result->user_data = state;
  result->type = VEF_RESULT_VALUE;
}

void vdf_count_postrun(vef_context_t *ctx, vef_postrun_args_t *args,
                       vef_postrun_result_t *result) {
  delete static_cast<CountState *>(args->user_data);
}

void vdf_count_clear(vef_context_t *ctx, vef_vdf_args_t *args) {
  auto *state = static_cast<CountState *>(args->user_data);
  state->count = 0;
}

void vdf_count_add(vef_context_t *ctx, vef_vdf_args_t *args,
                   vef_vdf_result_t *result) {
  auto *state = static_cast<CountState *>(args->user_data);
  vef_invalue_t val = villagesql::func_builder::get_invalue(ctx, args, 0);
  if (!val.is_null) {
    state->count++;
  }
}

void vdf_count_result_impl(vef_context_t *ctx, vef_vdf_args_t *args,
                           vef_vdf_result_t *out) {
  auto *state = static_cast<CountState *>(args->user_data);
  out->int_value = state->count;
  out->type = VEF_RESULT_VALUE;
}

// =============================================================================
// simple_double: scalar function that doubles an INT value (for mixed testing)
// =============================================================================

void simple_double_impl(vef_context_t *ctx, vef_invalue_t *input,
                        vef_vdf_result_t *out) {
  if (input->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  out->int_value = input->int_value * 2;
  out->type = VEF_RESULT_VALUE;
}

// =============================================================================
// Extension registration
// =============================================================================

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest")
        .func(make_func<&vdf_sum_result_impl>("vdf_sum")
                  .returns(INT)
                  .param(INT)
                  .clear<&vdf_sum_clear>()
                  .add<&vdf_sum_add>()
                  .prerun<&vdf_sum_prerun>()
                  .postrun<&vdf_sum_postrun>()
                  .build())
        .func(make_func<&vdf_count_result_impl>("vdf_count")
                  .returns(INT)
                  .param(INT)
                  .clear<&vdf_count_clear>()
                  .add<&vdf_count_add>()
                  .prerun<&vdf_count_prerun>()
                  .postrun<&vdf_count_postrun>()
                  .build())
        .func(make_func<&simple_double_impl>("simple_double")
                  .returns(INT)
                  .param(INT)
                  .build()))

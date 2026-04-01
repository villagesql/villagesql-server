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
//
// This extension demonstrates different ways to implement aggregate VDFs,
// ordered from most idiomatic to most manual:
//
// 1. vdf_sum(INT) -> INT: Typed API with std::optional<long long> as state.
//    Uses .state<T>() for automatic allocation, typed clear/accumulate
//    callbacks taking State&, and a result function returning
//    std::optional<long long> (nullopt -> SQL NULL).
//
// 2. vdf_count(INT) -> INT: Typed API with plain long long as state. Same
//    pattern as vdf_sum but the result is never NULL, so the result function
//    returns long long directly.
//
// 3. vdf_concat(STRING) -> STRING: Typed API with std::optional<std::string>
//    as state. Shows that the typed aggregate API works with string types.
//
// 4. vdf_max(INT) -> INT: Raw ABI with manual prerun/postrun and explicit
//    user_data casts. For authors who prefer direct control over state
//    management or need custom prerun/postrun logic beyond simple allocation.
//
// Also includes simple_double(INT) -> INT as a scalar VDF for testing mixed
// scalar/aggregate queries.

#include <villagesql/vsql.h>

#include <optional>
#include <string>

using namespace vsql;

// vdf_sum: aggregate that sums INT values, returns NULL for empty groups.
// State is optional<long long> — nullopt means no non-NULL values seen.

using SumState = std::optional<long long>;

void vdf_sum_clear(SumState &state) { state = std::nullopt; }

void vdf_sum_accumulate(SumState &state, IntArg val) {
  if (!val.is_null()) {
    state = state.value_or(0) + val.value();
  }
}

std::optional<long long> vdf_sum_result(const SumState &state) { return state; }

// vdf_count: aggregate that counts non-NULL INT values (always returns a value)

using CountState = long long;

void vdf_count_clear(CountState &state) { state = 0; }

void vdf_count_accumulate(CountState &state, IntArg val) {
  if (!val.is_null()) {
    state++;
  }
}

long long vdf_count_result(const CountState &state) { return state; }

// vdf_concat: aggregate that concatenates STRING values with commas.
// Returns NULL for empty groups.

using ConcatState = std::optional<std::string>;

void vdf_concat_clear(ConcatState &state) { state = std::nullopt; }

void vdf_concat_accumulate(ConcatState &state, StringArg val) {
  if (!val.is_null()) {
    if (state.has_value()) {
      state->append(",");
      state->append(val.value());
    } else {
      state = std::string(val.value());
    }
  }
}

std::optional<std::string> vdf_concat_result(const ConcatState &state) {
  return state;
}

// vdf_max: aggregate that returns the maximum INT value.
// Uses raw ABI callbacks with manual prerun/postrun instead of the typed API,
// for authors who prefer explicit control over state management.

struct MaxState {
  long long max_val = 0;
  bool has_value = false;
};

void vdf_max_prerun(vef_context_t *, vef_prerun_args_t *,
                    vef_prerun_result_t *result) {
  result->user_data = new MaxState{};
  result->type = VEF_RESULT_VALUE;
}

void vdf_max_postrun(vef_context_t *, vef_postrun_args_t *args,
                     vef_postrun_result_t *) {
  delete static_cast<MaxState *>(args->user_data);
}

void vdf_max_clear(vef_context_t *, vef_vdf_args_t *args) {
  auto *state = static_cast<MaxState *>(args->user_data);
  state->max_val = 0;
  state->has_value = false;
}

void vdf_max_accumulate(vef_context_t *ctx, vef_vdf_args_t *args,
                        vef_vdf_result_t *) {
  auto *state = static_cast<MaxState *>(args->user_data);
  vef_invalue_t val = villagesql::func_builder::get_invalue(ctx, args, 0);
  if (!val.is_null) {
    if (!state->has_value || val.int_value > state->max_val) {
      state->max_val = val.int_value;
      state->has_value = true;
    }
  }
}

void vdf_max_result(vef_context_t *, vef_vdf_args_t *args,
                    vef_vdf_result_t *out) {
  auto *state = static_cast<MaxState *>(args->user_data);
  if (!state->has_value) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  out->int_value = state->max_val;
  out->type = VEF_RESULT_VALUE;
}

// Scalar function that doubles an INT value (for mixed testing)
void simple_double_impl(IntArg input, IntResult out) {
  if (input.is_null()) {
    out.set_null();
    return;
  }
  out.set(input.value() * 2);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest")
        .func(make_func<&vdf_sum_result>("vdf_sum")
                  .returns(INT)
                  .param(INT)
                  .state<SumState>()
                  .clear<&vdf_sum_clear>()
                  .accumulate<&vdf_sum_accumulate>()
                  .build())
        .func(make_func<&vdf_count_result>("vdf_count")
                  .returns(INT)
                  .param(INT)
                  .state<CountState>()
                  .clear<&vdf_count_clear>()
                  .accumulate<&vdf_count_accumulate>()
                  .build())
        .func(make_func<&vdf_concat_result>("vdf_concat")
                  .returns(STRING)
                  .param(STRING)
                  .state<ConcatState>()
                  .clear<&vdf_concat_clear>()
                  .accumulate<&vdf_concat_accumulate>()
                  .build())
        .func(make_func<&vdf_max_result>("vdf_max")
                  .returns(INT)
                  .param(INT)
                  .prerun<&vdf_max_prerun>()
                  .postrun<&vdf_max_postrun>()
                  .clear<&vdf_max_clear>()
                  .accumulate<&vdf_max_accumulate>()
                  .build())
        .func(make_func<&simple_double_impl>("simple_double")
                  .returns(INT)
                  .param(INT)
                  .build()))

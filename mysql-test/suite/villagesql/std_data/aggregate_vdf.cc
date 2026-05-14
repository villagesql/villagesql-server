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
// This extension demonstrates different aggregate VDF implementations using
// make_aggregate_func. All three callbacks use typed C++ signatures:
//
// 1. vdf_sum(INT) -> INT: std::optional<long long> state (nullable result).
// 2. vdf_count(INT) -> INT: plain long long state (non-nullable result).
// 3. vdf_concat(STRING) -> STRING: std::optional<std::string> state.
// 4. vdf_max(INT) -> INT: struct state with typed clear/accumulate/result.
//
// Also includes simple_double(INT) -> INT as a scalar VDF for mixed testing.

#include <villagesql/vsql.h>

#include <optional>
#include <string>

using namespace vsql;

// vdf_sum: aggregate that sums INT values, returns NULL for empty groups.

using SumState = std::optional<long long>;

void vdf_sum_clear(SumState &state) { state = std::nullopt; }

void vdf_sum_accumulate(SumState &state, IntArg val) {
  if (!val.is_null()) {
    state = state.value_or(0) + val.value();
  }
}

void vdf_sum_result(const SumState &state, IntResult out) {
  if (!state.has_value()) {
    out.set_null();
    return;
  }
  out.set(state.value());
}

// vdf_count: aggregate that counts non-NULL INT values (always returns a value)

using CountState = long long;

void vdf_count_clear(CountState &state) { state = 0; }

void vdf_count_accumulate(CountState &state, IntArg val) {
  if (!val.is_null()) {
    state++;
  }
}

void vdf_count_result(const CountState &state, IntResult out) {
  out.set(state);
}

// vdf_concat: aggregate that concatenates STRING values with commas.

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

void vdf_concat_result(const ConcatState &state, StringResult out) {
  if (!state.has_value()) {
    out.set_null();
    return;
  }
  out.set(state.value());
}

// vdf_max: aggregate that returns the maximum INT value.

struct MaxState {
  long long max_val = 0;
  bool has_value = false;
};

void vdf_max_clear(MaxState &state) {
  state.max_val = 0;
  state.has_value = false;
}

void vdf_max_accumulate(MaxState &state, IntArg val) {
  if (!val.is_null()) {
    if (!state.has_value || val.value() > state.max_val) {
      state.max_val = val.value();
      state.has_value = true;
    }
  }
}

void vdf_max_result(const MaxState &state, IntResult out) {
  if (!state.has_value) {
    out.set_null();
    return;
  }
  out.set(state.max_val);
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
    make_extension()
        .func(make_aggregate_func<SumState, &vdf_sum_result>("vdf_sum")
                  .returns(INT)
                  .param(INT)
                  .clear<&vdf_sum_clear>()
                  .accumulate<&vdf_sum_accumulate>()
                  .build())
        .func(make_aggregate_func<CountState, &vdf_count_result>("vdf_count")
                  .returns(INT)
                  .param(INT)
                  .clear<&vdf_count_clear>()
                  .accumulate<&vdf_count_accumulate>()
                  .build())
        .func(make_aggregate_func<ConcatState, &vdf_concat_result>("vdf_concat")
                  .returns(STRING)
                  .param(STRING)
                  .clear<&vdf_concat_clear>()
                  .accumulate<&vdf_concat_accumulate>()
                  .build())
        .func(make_aggregate_func<MaxState, &vdf_max_result>("vdf_max")
                  .returns(INT)
                  .param(INT)
                  .clear<&vdf_max_clear>()
                  .accumulate<&vdf_max_accumulate>()
                  .build())
        .func(make_func<&simple_double_impl>("simple_double")
                  .returns(INT)
                  .param(INT)
                  .build()))

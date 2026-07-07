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

// Test extension for STRING-returning VDF result field sizing (issue #716).
//
// bounded_str declares .max_result_length(1000), so its result column is text.
// unbounded_str declares nothing, so its result column defaults to the argument
// width. bounded_concat is an aggregate with .max_result_length(5000). huge_str
// declares a length above MAX_BLOB_WIDTH to exercise the server-side cap.
// varargs_str_join uses both .varargs() and .max_result_length(200) together,
// verifying that required_protocol() returns VEF_PROTOCOL_4 for the combined
// case.

#include <villagesql/vsql.h>

#include <optional>
#include <string>

using namespace vsql;

void bounded_str_impl(IntArg n, StringResult out) {
  if (n.is_null()) {
    out.set_null();
    return;
  }
  out.set(std::string(static_cast<size_t>(n.value()), 'x'));
}

void unbounded_str_impl(IntArg n, StringResult out) {
  if (n.is_null()) {
    out.set_null();
    return;
  }
  out.set(std::string(static_cast<size_t>(n.value()), 'y'));
}

// huge_str: declares max_result_length far above MAX_BLOB_WIDTH (16777216) to
// exercise the server-side cap; the result column becomes longtext.
void huge_str_impl(IntArg n, StringResult out) {
  if (n.is_null()) {
    out.set_null();
    return;
  }
  out.set(std::string(static_cast<size_t>(n.value()), 'z'));
}

// bounded_concat: aggregate that concatenates STRING values. Declares
// max_result_length(5000) so the materialized column is sized for the full
// result, not the argument width.
using BoundedConcatState = std::optional<std::string>;

void bounded_concat_clear(BoundedConcatState &state) { state = std::nullopt; }

void bounded_concat_accumulate(BoundedConcatState &state, StringArg val) {
  if (!val.is_null()) {
    if (state.has_value()) {
      state->push_back(',');
      state->append(val.value());
    } else {
      state = std::string(val.value());
    }
  }
}

void bounded_concat_result(const BoundedConcatState &state, StringResult out) {
  if (!state.has_value()) {
    out.set_null();
    return;
  }
  out.set(state.value());
}

// varargs_str_join: varargs STRING VDF that joins its arguments with commas.
// Declares max_result_length(200), exercising the varargs + max_result_length
// combined case where required_protocol() must return a protocol later than
// VEF_PROTOCOL_3). The result column should be varchar(200).
void varargs_str_join_impl(VarArgs args, StringResult out) {
  std::string result;
  for (size_t i = 0; i < args.size(); i++) {
    auto a = args[i];
    if (a.is_null()) {
      out.set_null();
      return;
    }
    if (i > 0) result += ',';
    auto sv = a.as_str();
    result.append(sv.data(), sv.size());
  }
  out.set(result);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&bounded_str_impl>("bounded_str")
                  .returns(STRING)
                  .param(INT)
                  .max_result_length(1000)
                  .build())
        .func(make_func<&unbounded_str_impl>("unbounded_str")
                  .returns(STRING)
                  .param(INT)
                  .build())
        .func(make_func<&huge_str_impl>("huge_str")
                  .returns(STRING)
                  .param(INT)
                  .max_result_length(20000000)
                  .build())
        .func(make_aggregate_func<BoundedConcatState, &bounded_concat_result>(
                  "bounded_concat")
                  .returns(STRING)
                  .param(STRING)
                  .max_result_length(5000)
                  .clear<&bounded_concat_clear>()
                  .accumulate<&bounded_concat_accumulate>()
                  .build())
        .func(make_func<&varargs_str_join_impl>("varargs_str_join")
                  .returns(STRING)
                  .varargs()
                  .max_result_length(200)
                  .build()))

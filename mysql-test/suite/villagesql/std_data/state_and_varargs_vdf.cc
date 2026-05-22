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

// Test-only extension exercising VDF behaviors that combine the typed
// prerun/postrun + state machinery (#551) with the varargs machinery (#563):
//
//   counter_add(amount):       state-style VDF with an SQL parameter.
//                              Adds amount to a per-statement running total
//                              and returns the new total. Demonstrates
//                              WrapperTypedState forwarding SQL args
//                              alongside State&.
//
//   varargs_indexed(b...):     varargs VDF that reads per-statement state set
//                              by a typed prerun via VarArgs::state<T>().
//                              Combines #551 (set_user_data) with #563
//                              (varargs). Accepts CUSTOM-typed inputs (e.g.
//                              bytearray columns from another extension) and
//                              returns "<row#>:" prefixed to the concatenation
//                              of the raw bytes.

#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>

using namespace vsql;

struct CallCounter {
  long long n = 0;
};

static void counter_prerun(PrerunArgs, PrerunResult out) {
  out.set_user_data(new CallCounter{});
}

static void counter_postrun(PostrunArgs args) {
  args.delete_state<CallCounter>();
}

void counter_add(CallCounter &state, IntArg amount, IntResult out) {
  state.n += amount.value();
  out.set(state.n);
}

void varargs_indexed_prerun(PrerunArgs args, PrerunResult out) {
  if (args.size() == 0) {
    out.error("varargs_indexed requires at least one argument");
    return;
  }
  for (size_t i = 0; i < args.size(); i++) {
    auto t = args.type_at(i);
    // Allow CUSTOM (real values) and STRING (so NULL literals — which arrive
    // typed as STRING — also pass prerun). The body re-checks per-row.
    if (!t.is_custom() && !t.is_str()) {
      out.error("varargs_indexed: argument " + std::to_string(i) +
                " must be a CUSTOM-typed value");
      return;
    }
  }
  // Buffer: "NNNN:" prefix plus up to 64 bytes per argument.
  out.request_buffer_size(32 + args.size() * 64);
  out.set_user_data(new CallCounter{});
}

void varargs_indexed(VarArgs args, StringResult out) {
  CallCounter *state = args.state<CallCounter>();
  state->n++;
  auto dst = out.buffer();
  size_t off = static_cast<size_t>(snprintf(
      reinterpret_cast<char *>(dst.data()), dst.size(), "%lld:", state->n));
  for (auto a : args) {
    if (a.is_null() || !a.is_custom()) {
      out.set_null();
      return;
    }
    auto bytes = a.as_custom();
    size_t copy = bytes.size();
    if (off + copy > dst.size()) copy = dst.size() - off;
    memcpy(dst.data() + off, bytes.data(), copy);
    off += copy;
  }
  out.set_length(off);
}

void varargs_indexed_postrun(PostrunArgs args) {
  args.delete_state<CallCounter>();
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&counter_add>("counter_add")
                  .returns(INT)
                  .param(INT)
                  .prerun<&counter_prerun>()
                  .postrun<&counter_postrun>()
                  .build())
        .func(make_func<&varargs_indexed>("varargs_indexed")
                  .returns(STRING)
                  .varargs()
                  .prerun<&varargs_indexed_prerun>()
                  .postrun<&varargs_indexed_postrun>()
                  .build()))

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

// v2 of the update_test extension. Paired with update-test-v1.
// COUNTER type: same 4-byte storage as v1 (binary compatible).
// The auto-registered "COUNTER::to_string" VDF produces "v2:N"
// (changed from v1).
// counter_double(x) now returns x * 3 (changed from v1's x * 2).
// Sys var `update_test.threshold`: same shape as v1 (INT, 0..1000) so
// persisted values from v1 carry over cleanly through a happy-path
// update.

#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>

using namespace vsql;
namespace sv = vsql::preview_sys_var;

static const size_t kCounterLen = 4;

void counter_from_string(std::string_view from, vsql::CustomResult out) {
  char temp[64];
  size_t copy_len =
      from.size() < sizeof(temp) - 1 ? from.size() : sizeof(temp) - 1;
  memcpy(temp, from.data(), copy_len);
  temp[copy_len] = '\0';
  int val = 0;
  sscanf(temp, "%d", &val);
  auto buf = out.buffer();
  memcpy(buf.data(), &val, kCounterLen);
  out.set_length(kCounterLen);
}

// v2's to_string emits "v2:N" (vs v1's "v1:N") so tests can observe
// which version's decoder is wired in.
void counter_to_string(vsql::CustomArg in, vsql::StringResult out) {
  if (in.is_null()) {
    out.set_length(0);
    return;
  }
  auto data = in.value();
  int val = 0;
  memcpy(&val, data.data(), kCounterLen);
  auto buf = out.buffer();
  int n = snprintf(buf.data(), buf.size(), "v2:%d", val);
  if (n < 0) n = 0;
  out.set_length(static_cast<size_t>(n));
}

int counter_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto da = a.value();
  auto db = b.value();
  int va = 0, vb = 0;
  memcpy(&va, da.data(), kCounterLen);
  memcpy(&vb, db.data(), kCounterLen);
  return (va > vb) - (va < vb);
}

// counter_double(COUNTER) -> INT: returns value * 3 (changed from v1's * 2).
void counter_double(vsql::CustomArg in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();
  int val = 0;
  memcpy(&val, data.data(), kCounterLen);
  out.set(static_cast<long long>(val) * 3);
}

static long long g_threshold;

static auto SYS_VARS = sv::make_capability({
    sv::make_int("threshold", "v2 threshold: INT in range [0, 1000]",
                 &g_threshold, 0, 0, 1000),
});

static constexpr const char kCounterTypeName[] = "COUNTER";

constexpr auto COUNTER = vsql::make_type<kCounterTypeName>()
                             .persisted_length(kCounterLen)
                             .max_decode_buffer_length(32)
                             .from_string<&counter_from_string>()
                             .to_string<&counter_to_string>()
                             .compare<&counter_compare>()
                             .build();

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .type(COUNTER)
                              .func(make_func<&counter_double>("counter_double")
                                        .returns(INT)
                                        .param(COUNTER)
                                        .build())
                              .with(SYS_VARS))

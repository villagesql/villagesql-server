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

// v1 of the update_test extension. Paired with update-test-v2.
// COUNTER type: stores a single 32-bit integer (4 bytes).
// The auto-registered "COUNTER::to_string" VDF produces "v1:N".
// counter_double(x) returns x * 2.

#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>

using namespace vsql;
namespace sv = vsql::preview_sys_var;

static const size_t kCounterLen = 4;

// Encode: parse integer from string, store as 4-byte little-endian.
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

// Decode: read 4-byte little-endian int, produce "v1:N".
void counter_to_string(vsql::CustomArg in, vsql::StringResult out) {
  if (in.is_null()) {
    out.set_length(0);
    return;
  }
  auto data = in.value();
  int val = 0;
  memcpy(&val, data.data(), kCounterLen);
  auto buf = out.buffer();
  int n = snprintf(buf.data(), buf.size(), "v1:%d", val);
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

// counter_double(COUNTER) -> INT: returns value * 2.
void counter_double(vsql::CustomArg in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();
  int val = 0;
  memcpy(&val, data.data(), kCounterLen);
  out.set(static_cast<long long>(val) * 2);
}

static long long g_threshold;

// INT sys var in range [0, 1000]. Used by extension_update tests to
// exercise persisted-value behavior across version updates.
static auto SYS_VARS = sv::make_capability({
    sv::make_int("threshold", "v1 threshold: INT in range [0, 1000]",
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

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

// Bad-length variant of the update_test series. Mirrors v1's surface but
// changes COUNTER's persisted_length from 4 to 8 bytes. Used to exercise
// the retained-type storage-size compatibility check during update.

#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>

using namespace vsql;

static const size_t kCounterLen = 8;

void counter_from_string(std::string_view from, vsql::CustomResult out) {
  char temp[64];
  size_t copy_len =
      from.size() < sizeof(temp) - 1 ? from.size() : sizeof(temp) - 1;
  memcpy(temp, from.data(), copy_len);
  temp[copy_len] = '\0';
  long long val = 0;
  sscanf(temp, "%lld", &val);
  auto buf = out.buffer();
  memcpy(buf.data(), &val, kCounterLen);
  out.set_length(kCounterLen);
}

void counter_to_string(vsql::CustomArg in, vsql::StringResult out) {
  if (in.is_null()) {
    out.set_length(0);
    return;
  }
  auto data = in.value();
  long long val = 0;
  memcpy(&val, data.data(), kCounterLen);
  auto buf = out.buffer();
  int n = snprintf(buf.data(), buf.size(), "v1.1:%lld", val);
  if (n < 0) n = 0;
  out.set_length(static_cast<size_t>(n));
}

int counter_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto da = a.value();
  auto db = b.value();
  long long va = 0, vb = 0;
  memcpy(&va, da.data(), kCounterLen);
  memcpy(&vb, db.data(), kCounterLen);
  return (va > vb) - (va < vb);
}

void counter_double(vsql::CustomArg in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();
  long long val = 0;
  memcpy(&val, data.data(), kCounterLen);
  out.set(val * 2);
}

static constexpr const char kCounterTypeName[] = "COUNTER";

constexpr auto COUNTER = vsql::make_type<kCounterTypeName>()
                             .persisted_length(kCounterLen)
                             .max_decode_buffer_length(32)
                             .from_string<&counter_from_string>()
                             .to_string<&counter_to_string>()
                             .compare<&counter_compare>()
                             .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(COUNTER).func(
    make_func<&counter_double>("counter_double")
        .returns(INT)
        .param(COUNTER)
        .build()))

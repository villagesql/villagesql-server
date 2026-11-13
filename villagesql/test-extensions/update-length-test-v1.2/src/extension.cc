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

// Variable-length grow variant of the update_length_test series. VAR's
// max_persisted_length rises from 64 (v1) to 128, but grow is rejected.
// Existing columns keep a backing field sized to the old maximum.
//
// Both types share byte-copying helpers: only their storage length
// matters to the pre-check, and no test ever stores a value.

#include <villagesql/vsql.h>

#include <cstring>
#include <string_view>

using namespace vsql;

static constexpr int64_t kMaxDecodeLen = 256;

// Copies the input bytes, truncated to the buffer. A fixed-length value fills
// the whole (zero-padded) buffer; a variable-length value is at least 1 byte.
static size_t copy_into(std::string_view from, vsql::CustomResult &out) {
  auto buf = out.buffer();
  memset(buf.data(), 0, buf.size());
  size_t n = from.size() < buf.size() ? from.size() : buf.size();
  memcpy(buf.data(), from.data(), n);
  return n;
}

void fixed_from_string(std::string_view from, vsql::CustomResult out) {
  copy_into(from, out);
  out.set_length(out.buffer().size());
}

void var_from_string(std::string_view from, vsql::CustomResult out) {
  size_t n = copy_into(from, out);
  out.set_length(n > 0 ? n : 1);
}

void bytes_to_string(vsql::CustomArg in, vsql::StringResult out) {
  if (in.is_null()) {
    out.set_length(0);
    return;
  }
  auto data = in.value();
  auto buf = out.buffer();
  size_t n = data.size() < buf.size() ? data.size() : buf.size();
  memcpy(buf.data(), data.data(), n);
  out.set_length(n);
}

int bytes_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t n = va.size() < vb.size() ? va.size() : vb.size();
  int c = memcmp(va.data(), vb.data(), n);
  if (c != 0) return c;
  return (va.size() > vb.size()) - (va.size() < vb.size());
}

static constexpr const char kFixTypeName[] = "FIX";
static constexpr const char kVarTypeName[] = "VAR";

constexpr auto FIX = vsql::make_type<kFixTypeName>()
                         .persisted_length(8)
                         .max_decode_buffer_length(kMaxDecodeLen)
                         .from_string<&fixed_from_string>()
                         .to_string<&bytes_to_string>()
                         .compare<&bytes_compare>()
                         .build();

constexpr auto VAR = vsql::make_type<kVarTypeName>()
                         .variable_length_type()
                         .max_persisted_length(128)
                         .max_decode_buffer_length(kMaxDecodeLen)
                         .from_string<&var_from_string>()
                         .to_string<&bytes_to_string>()
                         .compare<&bytes_compare>()
                         .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(FIX).type(VAR))

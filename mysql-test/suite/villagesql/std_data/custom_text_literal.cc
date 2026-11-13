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

// TEXT16: up to 16 bytes of free text, zero padded. Its text form is the
// bytes it stores, from_string or from_binary, so a value can hold the quote
// and backslash characters that SQL literals escape.

#include <villagesql/vsql.h>

#include <cstddef>
#include <cstring>
#include <string_view>

using namespace vsql;

static const size_t kText16Len = 16;

static void store(const unsigned char *data, size_t len, CustomResult out) {
  auto buf = out.buffer();
  if (len > kText16Len || buf.size() < kText16Len) return;
  memset(buf.data(), 0, kText16Len);
  memcpy(buf.data(), data, len);
  out.set_length(kText16Len);
}

void text16_from_string(std::string_view from, CustomResult out) {
  store(reinterpret_cast<const unsigned char *>(from.data()), from.size(), out);
}

void text16_from_binary(Span<const unsigned char> from, CustomResult out) {
  store(from.data(), from.size(), out);
}

void text16_to_string(CustomArg in, StringResult out) {
  auto data = in.value();
  size_t n = 0;
  while (n < data.size() && data[n] != 0) n++;
  out.set(std::string_view(reinterpret_cast<const char *>(data.data()), n));
}

int text16_compare(CustomArg a, CustomArg b) {
  return memcmp(a.value().data(), b.value().data(), kText16Len);
}

static constexpr const char kText16TypeName[] = "TEXT16";

constexpr auto TEXT16 = make_type<kText16TypeName>()
                            .persisted_length(kText16Len)
                            .max_decode_buffer_length(kText16Len)
                            .from_string<&text16_from_string>()
                            .from_binary<&text16_from_binary>()
                            .to_string<&text16_to_string>()
                            .compare<&text16_compare>()
                            .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(TEXT16));

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

// TESTTYPE implementation with short precision (%.6g format)
// Stores two doubles (real and imaginary) as 16 bytes.

#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>
#include <string_view>

static const size_t kTestTypeLen = 16;

bool encode_testtype(std::string_view from, vsql::Span<unsigned char> to,
                     size_t *length) {
  double real = 0, imag = 0;
  char temp[256];
  size_t copy_len =
      from.size() < sizeof(temp) - 1 ? from.size() : sizeof(temp) - 1;
  memcpy(temp, from.data(), copy_len);
  temp[copy_len] = '\0';
  sscanf(temp, " ( %lf , %lf )", &real, &imag);
  memcpy(to.data(), &real, 8);
  memcpy(to.data() + 8, &imag, 8);
  *length = kTestTypeLen;
  return false;
}

bool decode_testtype_short(vsql::Span<const unsigned char> from,
                           vsql::Span<char> to, size_t *to_length) {
  double real, imag;
  memcpy(&real, from.data(), 8);
  memcpy(&imag, from.data() + 8, 8);
  *to_length = snprintf(to.data(), to.size(), "(%.6g,%.6g)", real, imag);
  return false;
}

int cmp_testtype(vsql::Span<const unsigned char> a,
                 vsql::Span<const unsigned char> b) {
  return memcmp(a.data(), b.data(), kTestTypeLen);
}

static constexpr const char kTestTypeName[] = "TESTTYPE";

constexpr auto TESTTYPE = vsql::make_type<kTestTypeName>()
                              .persisted_length(kTestTypeLen)
                              .max_decode_buffer_length(64)
                              .from_string<&encode_testtype>()
                              .to_string<&decode_testtype_short>()
                              .compare<&cmp_testtype>()
                              .build();

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest").type(TESTTYPE))

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

// Test UDFs for COMPLEX3 type - provides FROM_STRING, TO_STRING, and COMPARE.
// Used to verify VEF function lookup from custom type.

#include <villagesql/vsql.h>

#include <cstring>
#include <string_view>

static const size_t kComplex3Len = 16;

// FROM_STRING: always returns 16 zero bytes
// This is used to verify the VEF lookup mechanism works
bool complex3_from_string(std::string_view from,
                          vsql::Span<unsigned char> buf, size_t *length) {
  (void)from;  // Unused - we always return zeros

  if (buf.size() < kComplex3Len) {
    *length = 0;
    return true;  // Error
  }

  memset(buf.data(), 0, kComplex3Len);
  *length = kComplex3Len;
  return false;  // Success
}

// TO_STRING: always return "(0,0)"
bool complex3_to_string(vsql::Span<const unsigned char> buf,
                        vsql::Span<char> to, size_t *to_length) {
  (void)buf;  // Unused - always return "(0,0)"

  const char *result = "(0,0)";
  size_t len = strlen(result);

  if (to.size() < len + 1) {
    return true;  // Error
  }

  memcpy(to.data(), result, len);
  *to_length = len;
  return false;  // Success
}

// COMPARE: always returns 0 (equal)
int complex3_compare(vsql::Span<const unsigned char> a,
                     vsql::Span<const unsigned char> b) {
  (void)a;
  (void)b;
  return 0;
}

static constexpr const char kComplex3TypeName[] = "COMPLEX3";

constexpr auto COMPLEX3 = vsql::make_type<kComplex3TypeName>()
                              .persisted_length(kComplex3Len)
                              .max_decode_buffer_length(64)
                              .from_string<&complex3_from_string>()
                              .to_string<&complex3_to_string>()
                              .compare<&complex3_compare>()
                              .build();

VEF_GENERATE_ENTRY_POINTS(
    make_extension("complex3_ext", "0.0.1-devtest").type(COMPLEX3))

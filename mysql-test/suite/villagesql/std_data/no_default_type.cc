// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// Test extension: a fixed-length type with no intrinsic default registered
// and an encode function that rejects empty string. Used to verify that
// CREATE TABLE fails when a type has no valid intrinsic default.

#include <villagesql/extension.h>

#include <cstddef>
#include <cstring>
#include <string_view>

static const int64_t kLen = 4;

// Encode: only accepts "(N)" format. Empty string and other invalid inputs
// fail.
bool no_default_encode(std::string_view from,
                       villagesql::Span<unsigned char> buffer, size_t *length) {
  unsigned int nn = 0;
  char tmp[64];
  size_t copy = from.size() < sizeof(tmp) - 1 ? from.size() : sizeof(tmp) - 1;
  memcpy(tmp, from.data(), copy);
  tmp[copy] = '\0';
  if (sscanf(tmp, "(%u)", &nn) != 1) return true;
  if (buffer.size() < static_cast<size_t>(kLen)) return true;
  buffer[0] = static_cast<unsigned char>(nn);
  buffer[1] = buffer[2] = buffer[3] = 0;
  *length = static_cast<size_t>(kLen);
  return false;
}

bool no_default_decode(villagesql::Span<const unsigned char> buffer,
                       villagesql::Span<char> out, size_t *out_len) {
  if (buffer.size() < static_cast<size_t>(kLen)) return true;
  int written = snprintf(out.data(), out.size(), "(%u)", buffer[0]);
  if (written < 0) return true;
  *out_len = static_cast<size_t>(written);
  return false;
}

int no_default_compare(villagesql::Span<const unsigned char>,
                       villagesql::Span<const unsigned char>) {
  return 0;
}

using namespace villagesql::extension_builder;
using namespace villagesql::type_builder;

VEF_GENERATE_ENTRY_POINTS(
    make_extension(VEF_EXTENSION_NAME, "0.0.1-devtest")
        .type(make_type("NO_DEFAULT_TYPE")
                  .persisted_length(kLen)
                  .max_decode_buffer_length(16)
                  .encode("no_default_encode")
                  .decode("no_default_decode")
                  .compare("no_default_compare")
                  .build())
        .func(make_type_encode<&no_default_encode>("no_default_encode",
                                                   "NO_DEFAULT_TYPE"))
        .func(make_type_decode<&no_default_decode>("no_default_decode",
                                                   "NO_DEFAULT_TYPE"))
        .func(make_type_compare<&no_default_compare>("no_default_compare",
                                                     "NO_DEFAULT_TYPE")))

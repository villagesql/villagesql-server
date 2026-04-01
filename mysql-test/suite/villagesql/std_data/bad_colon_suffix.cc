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

// Bad extension: registers type MYTYPE but names its encode VDF
// "MYTYPE::transform". The :: suffix "transform" is not a valid type method
// name, so INSTALL EXTENSION must fail.

#include <villagesql/vsql.h>

#include <cstring>

bool mytype_encode(std::string_view from, vsql::Span<unsigned char> buf,
                   size_t *length) {
  if (buf.size() < 4) return true;
  size_t to_copy = from.size() < buf.size() ? from.size() : buf.size();
  memcpy(buf.data(), from.data(), to_copy);
  *length = to_copy;
  return false;
}

bool mytype_decode(vsql::Span<const unsigned char> data,
                   vsql::Span<char> out, size_t *out_len) {
  if (data.size() == 0) return true;
  size_t to_copy = data.size() < out.size() ? data.size() : out.size();
  memcpy(out.data(), data.data(), to_copy);
  *out_len = to_copy;
  return false;
}

int mytype_compare(vsql::Span<const unsigned char> a,
                   vsql::Span<const unsigned char> b) {
  size_t min_len = a.size() < b.size() ? a.size() : b.size();
  int r = memcmp(a.data(), b.data(), min_len);
  if (r != 0) return r;
  if (a.size() < b.size()) return -1;
  if (a.size() > b.size()) return 1;
  return 0;
}

using namespace villagesql::extension_builder;
using namespace villagesql::func_builder;
using namespace villagesql::type_builder;

// MYTYPE is the type name, but the encode VDF is named "MYTYPE::transform".
// The suffix "transform" is not a valid type method (must be encode, decode,
// compare, or hash), so installation must fail.
VEF_GENERATE_ENTRY_POINTS(
    make_extension("bad_colon_suffix", "0.0.1-devtest")
        .type(make_type("MYTYPE")
                  .persisted_length(16)
                  .max_decode_buffer_length(64)
                  .encode("MYTYPE::transform")
                  .decode("MYTYPE::decode")
                  .compare("MYTYPE::compare")
                  .build())
        .func(make_type_encode<&mytype_encode>("MYTYPE::transform", "MYTYPE"))
        .func(make_type_decode<&mytype_decode>("MYTYPE::decode", "MYTYPE"))
        .func(make_type_compare<&mytype_compare>("MYTYPE::compare", "MYTYPE")))

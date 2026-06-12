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

// Bad-registration test extension: declares a variable-length type
// (persisted_length = -1) but never sets max_persisted_length. A
// variable-length type needs max_persisted_length to bound the backing field,
// so the server must reject this at INSTALL EXTENSION time with a clear error
// from build_type_descriptor_v4.

#include <villagesql/vsql.h>

#include <cstring>
#include <string_view>

void bad_var_encode(std::string_view from, vsql::CustomResult out) {
  (void)from;
  auto buf = out.buffer();
  if (buf.empty()) return;
  memset(buf.data(), 0, buf.size());
  out.set_length(buf.size());
}

void bad_var_decode(vsql::CustomArg in, vsql::StringResult out) {
  (void)in;
  out.set("");
}

int bad_var_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t n = va.size() < vb.size() ? va.size() : vb.size();
  return memcmp(va.data(), vb.data(), n);
}

static constexpr const char kBadVarTypeName[] = "BAD_VAR_NO_MAXLEN";

constexpr auto BAD_VAR_NO_MAXLEN = vsql::make_type<kBadVarTypeName>()
                                       .persisted_length(-1)
                                       .max_decode_buffer_length(16)
                                       .from_string<&bad_var_encode>()
                                       .to_string<&bad_var_decode>()
                                       .compare<&bad_var_compare>()
                                       .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension().type(BAD_VAR_NO_MAXLEN))

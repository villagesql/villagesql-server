// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// Bad-registration test extension: declares a type with persisted_length = -1
// (variable-length) but does not provide resolve_params.
//
// This fixture is built against the frozen stable v3 SDK (see the "v3" ABI
// selector in test-extensions/CMakeLists.txt), so the type registers at
// VEF_PROTOCOL_3 -- the v3 type builder does not raise variable-length types to
// VEF_PROTOCOL_4 the way the current dev SDK does. The server therefore
// dispatches it to build_type_descriptor_v3, which requires resolve_params for
// persisted_length == -1 and rejects this extension at INSTALL EXTENSION time.
// This is the rejection path a real v3-compiled extension would hit; the dev
// SDK can no longer produce a variable-length type that reaches it.

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

static constexpr const char kBadVarTypeName[] = "BAD_VAR_NO_RESOLVE";

constexpr auto BAD_VAR_NO_RESOLVE = vsql::make_type<kBadVarTypeName>()
                                        .persisted_length(-1)
                                        .max_decode_buffer_length(16)
                                        .from_string<&bad_var_encode>()
                                        .to_string<&bad_var_decode>()
                                        .compare<&bad_var_compare>()
                                        .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension().type(BAD_VAR_NO_RESOLVE))

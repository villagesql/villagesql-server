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

// Bad-registration test extension: a type names a collect aggregate VDF
// (VEF_PROTOCOL_6) that this extension does not register. The type register
// (build_type_descriptor_v5) must reject it with "... not found", so INSTALL
// EXTENSION fails.

#include <villagesql/vsql.h>

#include <cstring>
#include <string_view>

void bad_collect_encode(std::string_view from, vsql::CustomResult out) {
  (void)from;
  auto buf = out.buffer();
  if (buf.empty()) return;
  memset(buf.data(), 0, buf.size());
  out.set_length(buf.size());
}

void bad_collect_decode(vsql::CustomArg in, vsql::StringResult out) {
  (void)in;
  out.set("");
}

int bad_collect_compare(vsql::CustomArg a, vsql::CustomArg b) {
  return memcmp(a.value().data(), b.value().data(), a.value().size());
}

static constexpr const char kTypeName[] = "BAD_COLLECT_MISSING";

constexpr auto BAD_COLLECT_MISSING = vsql::make_type<kTypeName>()
                                         .persisted_length(8)
                                         .max_decode_buffer_length(16)
                                         .from_string<&bad_collect_encode>()
                                         .to_string<&bad_collect_decode>()
                                         .compare<&bad_collect_compare>()
                                         .collect_aggregate("no_such_collect")
                                         .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension().type(BAD_COLLECT_MISSING))

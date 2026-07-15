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

// Bad-registration test extension: registers two types with the SAME name
// ("DUP_TYPE"). The two names live in distinct NTTP constants so each type has
// its own VDF-name buffers, but the name strings collide, so registration must
// reject the extension at INSTALL EXTENSION time.

#include <villagesql/vsql.h>

#include <cstdint>
#include <cstring>
#include <string_view>

constexpr int64_t kDupSize = 8;

void dup_encode(std::string_view /*from*/, vsql::CustomResult out) {
  auto buf = out.buffer();
  if (buf.size() < static_cast<size_t>(kDupSize)) return;
  memset(buf.data(), 0, kDupSize);
  out.set_length(static_cast<size_t>(kDupSize));
}

void dup_decode(vsql::CustomArg /*in*/, vsql::StringResult out) { out.set(""); }

int dup_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t n = va.size() < vb.size() ? va.size() : vb.size();
  return memcmp(va.data(), vb.data(), n);
}

// Two distinct NTTP constants holding the same type name.
static constexpr const char kDupName1[] = "DUP_TYPE";
static constexpr const char kDupName2[] = "DUP_TYPE";

constexpr auto DUP_TYPE_1 = vsql::make_type<kDupName1>()
                                .persisted_length(kDupSize)
                                .max_decode_buffer_length(kDupSize)
                                .from_string<&dup_encode>()
                                .to_string<&dup_decode>()
                                .compare<&dup_compare>()
                                .build();

constexpr auto DUP_TYPE_2 = vsql::make_type<kDupName2>()
                                .persisted_length(kDupSize)
                                .max_decode_buffer_length(kDupSize)
                                .from_string<&dup_encode>()
                                .to_string<&dup_decode>()
                                .compare<&dup_compare>()
                                .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension().type(DUP_TYPE_1).type(DUP_TYPE_2))

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

// Fixture for the custom-type storage-size cap, fixed-length arm: HUGE_FIELD
// declares a persisted_length wider than a custom column can be backed by, so
// INSTALL EXTENSION must reject the extension and name the type.
//
// This is a whole extension for one type because registration stops at the
// first invalid type. Pairing it with the variable-length case in one VEB would
// leave that case unvalidated -- hence the sibling vsql-huge-var-test, which
// covers max_persisted_length through the v4 builder while this one covers
// persisted_length through v3.
//
// The type is never usable, so its operations only need to compile and be
// registrable; nothing ever invokes them.

#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>
#include <string_view>

constexpr int64_t kHugeFieldLen = 70000;  // far above the cap

void huge_encode(std::string_view /*from*/, vsql::CustomResult out) {
  auto buf = out.buffer();
  memset(buf.data(), 0, buf.size());
  out.set_length(buf.size());
}

void huge_decode(vsql::CustomArg in, vsql::StringResult out) {
  auto data = in.value();
  auto buf = out.buffer();
  int written = snprintf(buf.data(), buf.size(), "(%zu)", data.size());
  if (written < 0) return;
  out.set_length(static_cast<size_t>(written));
}

int huge_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t n = va.size() < vb.size() ? va.size() : vb.size();
  return memcmp(va.data(), vb.data(), n);
}

static constexpr const char kHugeFieldTypeName[] = "HUGE_FIELD";

constexpr auto HUGE_FIELD = vsql::make_type<kHugeFieldTypeName>()
                                .persisted_length(kHugeFieldLen)
                                .max_decode_buffer_length(16)
                                .from_string<&huge_encode>()
                                .to_string<&huge_decode>()
                                .compare<&huge_compare>()
                                .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension().type(HUGE_FIELD))

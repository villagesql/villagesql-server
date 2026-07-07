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

// v1 of the update_schema_test extension. Paired with update-schema-test-v2.
// Registers one custom type MYTYPE (4-byte little-endian int) so that
// v2 can demonstrate migrating an existing MYTYPE column to a new
// MYTYPE_V2 type with a different on-disk representation, via ALTER
// EXTENSION ... AT RESTART followed by application-driven ALTER TABLE.
// The auto-registered "MYTYPE::to_string" VDF produces "v1:N".

#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>

using namespace vsql;

static const size_t kMyTypeLen = 4;

// Encode: parse integer from string, store as 4-byte little-endian.
void mytype_from_string(std::string_view from, vsql::CustomResult out) {
  char temp[64];
  size_t copy_len =
      from.size() < sizeof(temp) - 1 ? from.size() : sizeof(temp) - 1;
  memcpy(temp, from.data(), copy_len);
  temp[copy_len] = '\0';
  int val = 0;
  sscanf(temp, "%d", &val);
  auto buf = out.buffer();
  memcpy(buf.data(), &val, kMyTypeLen);
  out.set_length(kMyTypeLen);
}

// Decode: read 4-byte little-endian int, produce "v1:N".
void mytype_to_string(vsql::CustomArg in, vsql::StringResult out) {
  if (in.is_null()) {
    out.set_length(0);
    return;
  }
  auto data = in.value();
  int val = 0;
  memcpy(&val, data.data(), kMyTypeLen);
  auto buf = out.buffer();
  int n = snprintf(buf.data(), buf.size(), "v1:%d", val);
  if (n < 0) n = 0;
  out.set_length(static_cast<size_t>(n));
}

int mytype_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto da = a.value();
  auto db = b.value();
  int va = 0, vb = 0;
  memcpy(&va, da.data(), kMyTypeLen);
  memcpy(&vb, db.data(), kMyTypeLen);
  return (va > vb) - (va < vb);
}

static constexpr const char kMyTypeName[] = "MYTYPE";

constexpr auto MYTYPE = vsql::make_type<kMyTypeName>()
                            .persisted_length(kMyTypeLen)
                            .max_decode_buffer_length(32)
                            .from_string<&mytype_from_string>()
                            .to_string<&mytype_to_string>()
                            .compare<&mytype_compare>()
                            .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(MYTYPE))

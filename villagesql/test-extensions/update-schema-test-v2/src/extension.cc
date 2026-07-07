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

// v2 of the update_schema_test extension. Paired with update-schema-test-v1.
//
// v2 keeps MYTYPE (same 4-byte little-endian int layout as v1, so any
// existing MYTYPE column keeps working after an ALTER EXTENSION version
// bump) and adds:
//   - MYTYPE_V2: a wider 8-byte little-endian int representation.
//   - mytype_to_v2(MYTYPE) -> MYTYPE_V2: conversion VDF the user calls
//     from ALTER TABLE to migrate an existing MYTYPE column's data to
//     the new MYTYPE_V2 representation.
//
// The auto-registered "MYTYPE::to_string" still produces "v1:N" (kept
// stable across versions), and the new "MYTYPE_V2::to_string" produces
// "v2:N" so tests can observe the column type transition.

#include <villagesql/vsql.h>

#include <cstdio>
#include <cstring>

using namespace vsql;

static const size_t kMyTypeLen = 4;
static const size_t kMyTypeV2Len = 8;

// MYTYPE (unchanged from v1): 4-byte little-endian int.

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

// MYTYPE_V2: 8-byte little-endian int. Wider representation demonstrates
// a real on-disk layout change from MYTYPE.

void mytype_v2_from_string(std::string_view from, vsql::CustomResult out) {
  char temp[64];
  size_t copy_len =
      from.size() < sizeof(temp) - 1 ? from.size() : sizeof(temp) - 1;
  memcpy(temp, from.data(), copy_len);
  temp[copy_len] = '\0';
  long long val = 0;
  sscanf(temp, "%lld", &val);
  auto buf = out.buffer();
  memcpy(buf.data(), &val, kMyTypeV2Len);
  out.set_length(kMyTypeV2Len);
}

void mytype_v2_to_string(vsql::CustomArg in, vsql::StringResult out) {
  if (in.is_null()) {
    out.set_length(0);
    return;
  }
  auto data = in.value();
  long long val = 0;
  memcpy(&val, data.data(), kMyTypeV2Len);
  auto buf = out.buffer();
  int n = snprintf(buf.data(), buf.size(), "v2:%lld", val);
  if (n < 0) n = 0;
  out.set_length(static_cast<size_t>(n));
}

int mytype_v2_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto da = a.value();
  auto db = b.value();
  long long va = 0, vb = 0;
  memcpy(&va, da.data(), kMyTypeV2Len);
  memcpy(&vb, db.data(), kMyTypeV2Len);
  return (va > vb) - (va < vb);
}

// mytype_to_v2(MYTYPE) -> MYTYPE_V2: widen the 4-byte value to 8 bytes.
// The return-type context (MYTYPE_V2) is supplied by the destination
// column at call time; this VDF just writes the bytes.
void mytype_to_v2(vsql::CustomArg in, vsql::CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();
  int narrow = 0;
  memcpy(&narrow, data.data(), kMyTypeLen);
  long long wide = static_cast<long long>(narrow);
  auto buf = out.buffer();
  memcpy(buf.data(), &wide, kMyTypeV2Len);
  out.set_length(kMyTypeV2Len);
}

static constexpr const char kMyTypeName[] = "MYTYPE";
static constexpr const char kMyTypeV2Name[] = "MYTYPE_V2";

constexpr auto MYTYPE = vsql::make_type<kMyTypeName>()
                            .persisted_length(kMyTypeLen)
                            .max_decode_buffer_length(32)
                            .from_string<&mytype_from_string>()
                            .to_string<&mytype_to_string>()
                            .compare<&mytype_compare>()
                            .build();

constexpr auto MYTYPE_V2 = vsql::make_type<kMyTypeV2Name>()
                               .persisted_length(kMyTypeV2Len)
                               .max_decode_buffer_length(48)
                               .from_string<&mytype_v2_from_string>()
                               .to_string<&mytype_v2_to_string>()
                               .compare<&mytype_v2_compare>()
                               .build();

VEF_GENERATE_ENTRY_POINTS(make_extension().type(MYTYPE).type(MYTYPE_V2).func(
    make_func<&mytype_to_v2>("mytype_to_v2")
        .returns(MYTYPE_V2)
        .param(MYTYPE)
        .deterministic()
        .build()))

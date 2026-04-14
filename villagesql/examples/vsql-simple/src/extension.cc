// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// VillageSQL extension demonstrating the vsql API.
//
// The BYTEARRAY type is a fixed 8-byte value.

#include <villagesql/vsql.h>

using namespace vsql;

static const size_t kBytearrayLen = 8;

// from_string: string -> binary (copy up to 8 bytes, space-pad)
bool bytearray_from_string(std::string_view from, Span<unsigned char> buf,
                           size_t *length) {
  if (buf.size() < kBytearrayLen) return true;  // error
  memset(buf.data(), ' ', kBytearrayLen);
  size_t copy_len = from.size() < kBytearrayLen ? from.size() : kBytearrayLen;
  if (copy_len > 0) memcpy(buf.data(), from.data(), copy_len);
  *length = kBytearrayLen;
  return false;  // success
}

// to_string: binary -> string (copy 8 bytes)
bool bytearray_to_string(Span<const unsigned char> data, Span<char> out,
                         size_t *out_len) {
  if (data.size() < kBytearrayLen || out.size() < kBytearrayLen) return true;
  memcpy(out.data(), data.data(), kBytearrayLen);
  *out_len = kBytearrayLen;
  return false;  // success
}

// Compare: lexicographic byte comparison
int bytearray_compare(Span<const unsigned char> a,
                      Span<const unsigned char> b) {
  return memcmp(a.data(), b.data(), kBytearrayLen);
}

// ROT13: apply ROT13 cipher to ASCII letters
void rot13(CustomArg in, CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto src = in.value();
  auto dst = out.buffer();
  for (size_t i = 0; i < kBytearrayLen && i < src.size(); i++) {
    unsigned char c = src[i];
    if (c >= 'A' && c <= 'Z') {
      dst[i] = 'A' + ((c - 'A' + 13) % 26);
    } else if (c >= 'a' && c <= 'z') {
      dst[i] = 'a' + ((c - 'a' + 13) % 26);
    } else {
      dst[i] = c;
    }
  }
  out.set_length(kBytearrayLen);
}

// EVEN_CHARS: extract bytes at positions 0, 2, 4, 6 (returns 4 bytes)
void even_chars(CustomArg in, CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto src = in.value();
  auto dst = out.buffer();
  memset(dst.data(), ' ', kBytearrayLen);
  if (src.size() >= kBytearrayLen) {
    dst[0] = src[0];
    dst[1] = src[2];
    dst[2] = src[4];
    dst[3] = src[6];
  }
  out.set_length(kBytearrayLen);
}

// ODD_CHARS: extract bytes at positions 1, 3, 5, 7 (returns 4 bytes)
void odd_chars(CustomArg in, CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto src = in.value();
  auto dst = out.buffer();
  memset(dst.data(), ' ', kBytearrayLen);
  if (src.size() >= kBytearrayLen) {
    dst[0] = src[1];
    dst[1] = src[3];
    dst[2] = src[5];
    dst[3] = src[7];
  }
  out.set_length(kBytearrayLen);
}

// BA_CONCAT: concatenate two bytearrays (returns STRING with 16 bytes)
void ba_concat(CustomArg a, CustomArg b, StringResult out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  auto dst = out.buffer();
  memset(dst.data(), ' ', kBytearrayLen * 2);
  memcpy(dst.data(), a.value().data(), kBytearrayLen);
  memcpy(dst.data() + kBytearrayLen, b.value().data(), kBytearrayLen);
  out.set_length(kBytearrayLen * 2);
}

static constexpr const char kBytearrayTypeName[] = "bytearray";

constexpr auto BYTEARRAY = vsql::make_type<kBytearrayTypeName>()
                               .persisted_length(kBytearrayLen)
                               .max_decode_buffer_length(kBytearrayLen)
                               .from_string<&bytearray_from_string>()
                               .to_string<&bytearray_to_string>()
                               .compare<&bytearray_compare>()
                               .build();

VEF_GENERATE_ENTRY_POINTS(make_extension("vsql_simple", "0.0.1")
                              .type(BYTEARRAY)
                              .func(make_func<&rot13>("rot13")
                                        .returns(BYTEARRAY)
                                        .param(BYTEARRAY)
                                        .build())
                              .func(make_func<&even_chars>("even_chars")
                                        .returns(BYTEARRAY)
                                        .param(BYTEARRAY)
                                        .build())
                              .func(make_func<&odd_chars>("odd_chars")
                                        .returns(BYTEARRAY)
                                        .param(BYTEARRAY)
                                        .build())
                              .func(make_func<&ba_concat>("ba_concat")
                                        .returns(STRING)
                                        .param(BYTEARRAY)
                                        .param(BYTEARRAY)
                                        .build()))

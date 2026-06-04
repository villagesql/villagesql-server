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

// Test fixture for issue #535: a variable-length, NON-parameterized vector
// type whose element count is decided per value.
//
// VARVEC stores a vector of little-endian int16 elements. It declares
// persisted_length = -1 with a max_persisted_length upper bound and NO
// params/int_to_params/resolve_params. A column is declared without any length
// or parameters:
//   CREATE TABLE t (v vsql_varvec_test.VARVEC);
// and each value keeps its own element count, like VARBINARY -- vectors of
// different lengths coexist in the same column. This contrasts with a
// parameterized vector (e.g. PVEC(N)), whose dimension is fixed per column by
// int_to_params/resolve_params.
//
// The backing field is a VARCHAR(max_persisted_length): the in-memory buffer is
// sized to the upper bound, but only the actual encoded bytes (set via
// out.set_length()) are written, and the VARCHAR length prefix records the
// per-value length. The element count is recovered on decode from the value's
// byte length, not from a type parameter.

#include <villagesql/vsql.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>

// Upper bound on a stored VARVEC value: at most kVarvecMaxElems int16 elements,
// so kVarvecMaxElems * 2 bytes. Sizes the backing field and the encode buffer.
constexpr int64_t kVarvecMaxElems = 256;
constexpr int64_t kVarvecMaxLen = kVarvecMaxElems * 2;
// Upper bound on the text form "[v1,v2,...]": each int16 prints as at most
// "-32768," (7 chars), plus the surrounding brackets.
constexpr int64_t kVarvecMaxText = kVarvecMaxElems * 8 + 2;

static void store_i16(unsigned char *buf, int16_t v) {
  buf[0] = static_cast<unsigned char>(v & 0xFF);
  buf[1] = static_cast<unsigned char>((v >> 8) & 0xFF);
}

static int16_t load_i16(const unsigned char *buf) {
  return static_cast<int16_t>(static_cast<uint16_t>(buf[0]) |
                              (static_cast<uint16_t>(buf[1]) << 8));
}

// STRING -> binary: parse "[v1,v2,...]" into little-endian int16 elements. The
// element count is decided per value (not a type parameter); the actual byte
// length is reported via out.set_length(). The buffer is sized to the type's
// max_persisted_length, so max_elems below is the per-column capacity.
void varvec_from_string(std::string_view from, vsql::CustomResult out) {
  auto buf = out.buffer();
  const size_t max_elems = buf.size() / 2;

  std::string input(from);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    out.warning("VARVEC: expected '['");
    return;
  }
  s++;

  size_t count = 0;
  while (*s != '\0') {
    while (*s == ' ') s++;
    if (*s == ']') break;
    if (count >= max_elems) {
      out.warning("VARVEC: too many elements");
      return;
    }
    char *endptr = nullptr;
    long val = strtol(s, &endptr, 10);
    if (endptr == s) {
      out.warning("VARVEC: parse error");
      return;
    }
    store_i16(buf.data() + count * 2, static_cast<int16_t>(val));
    count++;
    s = endptr;
    while (*s == ' ') s++;
    if (*s == ',') s++;
  }
  if (*s != ']') {
    out.warning("VARVEC: missing ']'");
    return;
  }
  out.set_length(count * 2);
}

// binary -> STRING: the element count is derived from the value's byte length
// (data.size() / 2), since this type has no dimension parameter.
void varvec_to_string(vsql::CustomArg in, vsql::StringResult out) {
  auto data = in.value();
  const size_t count = data.size() / 2;
  auto buf = out.buffer();
  size_t pos = 0;
  if (pos >= buf.size()) return;
  buf[pos++] = '[';
  for (size_t i = 0; i < count; i++) {
    if (i > 0) {
      if (pos >= buf.size()) return;
      buf[pos++] = ',';
    }
    int16_t v = load_i16(data.data() + i * 2);
    int written =
        snprintf(buf.data() + pos, buf.size() - pos, "%d", static_cast<int>(v));
    if (written < 0 || pos + static_cast<size_t>(written) >= buf.size()) return;
    pos += static_cast<size_t>(written);
  }
  if (pos >= buf.size()) return;
  buf[pos++] = ']';
  out.set_length(pos);
}

// Lexicographic element-wise comparison; a shorter vector sorts first on a
// common prefix (like VARBINARY).
int varvec_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t na = va.size() / 2;
  size_t nb = vb.size() / 2;
  size_t n = na < nb ? na : nb;
  for (size_t i = 0; i < n; i++) {
    int16_t ea = load_i16(va.data() + i * 2);
    int16_t eb = load_i16(vb.data() + i * 2);
    if (ea != eb) return ea < eb ? -1 : 1;
  }
  if (na != nb) return na < nb ? -1 : 1;
  return 0;
}

// Number of elements in this VARVEC value -- derived per value from the stored
// byte length, the defining trait of a variable-length, non-parameterized type.
void varvec_dim(vsql::CustomArg in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(in.value().size() / 2));
}

// Concatenate two VARVEC values into one: (VARVEC, VARVEC) -> VARVEC. The two
// inputs may have different element counts; the result holds all elements of a
// followed by all elements of b, so its byte length is the sum of the inputs'
// byte lengths. Because each VARVEC value is already a packed sequence of
// little-endian int16 elements, concatenation is just a byte-wise append.
//
// This is a CUSTOM-returning VDF whose output is the SUM of two variable-length
// inputs: the result buffer is sized to the return type's max_persisted_length,
// so a concatenation that exceeds that capacity is reported as a warning rather
// than silently truncated.
void varvec_concat(vsql::CustomArg a, vsql::CustomArg b,
                   vsql::CustomResult out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  auto va = a.value();
  auto vb = b.value();
  auto buf = out.buffer();
  const size_t total = va.size() + vb.size();
  if (total > buf.size()) {
    out.warning("VARVEC: concatenated value exceeds max length");
    return;
  }
  if (va.size() > 0) memcpy(buf.data(), va.data(), va.size());
  if (vb.size() > 0) memcpy(buf.data() + va.size(), vb.data(), vb.size());
  out.set_length(total);
}

static constexpr const char kVarvecTypeName[] = "VARVEC";

constexpr auto VARVEC = vsql::make_type<kVarvecTypeName>()
                            .persisted_length(-1)
                            .max_persisted_length(kVarvecMaxLen)
                            .max_decode_buffer_length(kVarvecMaxText)
                            .from_string<&varvec_from_string>()
                            .to_string<&varvec_to_string>()
                            .compare<&varvec_compare>()
                            .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .type(VARVEC)
                              .func(make_func<&varvec_dim>("varvec_dim")
                                        .returns(INT)
                                        .param(VARVEC)
                                        .deterministic()
                                        .build())
                              .func(make_func<&varvec_concat>("varvec_concat")
                                        .returns(VARVEC)
                                        .param(VARVEC)
                                        .param(VARVEC)
                                        .deterministic()
                                        .build()))

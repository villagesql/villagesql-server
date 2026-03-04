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

// VillageSQL TVECTOR extension demonstrating parameterized custom types.
//
// The TVECTOR(N) type stores fixed-dimension float32 vectors.
// TVECTOR(3) means 3 float32 elements, stored as 12 bytes.
// Text format: "[1.0,2.0,3.0]" (comma-separated floats in brackets)

#include <villagesql/extension.h>

#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>

using villagesql::BinaryArg;
using villagesql::BinaryResult;
using villagesql::IntResult;
using villagesql::StringArg;
using villagesql::StringResult;

// Little-endian float32 store/load helpers.

void store_float(unsigned char *buf, float val) {
  uint32_t bits;
  memcpy(&bits, &val, sizeof(float));
  buf[0] = static_cast<unsigned char>(bits);
  buf[1] = static_cast<unsigned char>(bits >> 8);
  buf[2] = static_cast<unsigned char>(bits >> 16);
  buf[3] = static_cast<unsigned char>(bits >> 24);
}

float load_float(const unsigned char *buf) {
  uint32_t bits = static_cast<uint32_t>(buf[0]) |
                  (static_cast<uint32_t>(buf[1]) << 8) |
                  (static_cast<uint32_t>(buf[2]) << 16) |
                  (static_cast<uint32_t>(buf[3]) << 24);
  float val;
  memcpy(&val, &bits, sizeof(float));
  return val;
}

// Convert TYPE(N) integer to parameter key-value pairs.
// Populates params map with {"dimension": "<N>"}.
bool tvector_int_to_params(int64_t value,
                           std::map<std::string, std::string> &params,
                           char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR dimension must be a positive integer, got %" PRId64,
             value);
    return true;
  }
  params["dimension"] = std::to_string(value);
  return false;
}

// Validate type parameters and compute storage characteristics.
bool tvector_resolve_params(const std::map<std::string, std::string> &params,
                            villagesql::ResolvedTypeParams *result,
                            char *error_msg) {
  auto it = params.find("dimension");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: dimension must be a positive integer");
    return true;
  }
  char *endptr = nullptr;
  int64_t dimension = strtoll(it->second.c_str(), &endptr, 10);
  if (endptr == it->second.c_str() || *endptr != '\0') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: invalid dimension value '%s'", it->second.c_str());
    return true;
  }
  if (dimension <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: dimension must be a positive integer");
    return true;
  }
  result->persisted_length = dimension * 4;
  result->max_decode_buffer_length = dimension * 16;
  return false;
}

// Encode: "[f1,f2,...,fN]" -> N * 4 bytes binary.
// STRING -> TVECTOR
// Dimension is inferred from out.buffer().size() / 4.
void tvector_from_string(StringArg in, BinaryResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }

  auto dst = out.buffer();
  if (dst.size() == 0) {
    out.error("response buffer too small");
    return;
  }

  size_t dimension = dst.size() / 4;

  // strtof requires a null-terminated string.
  std::string input(in.value());
  const char *p = input.c_str();

  // Skip leading whitespace
  while (*p == ' ') p++;
  if (*p != '[') {
    out.error("expected '[' at start of vector");
    return;
  }
  p++;

  size_t count = 0;
  while (*p != '\0') {
    // Skip whitespace
    while (*p == ' ') p++;
    if (*p == ']') break;

    if (count >= dimension) {
      out.error("too many elements");
      return;
    }

    char *endptr = nullptr;
    float val = strtof(p, &endptr);
    if (endptr == p) {
      out.error("failed to parse float");
      return;
    }
    store_float(dst.data() + count * 4, val);
    count++;
    p = endptr;

    // Skip whitespace and comma
    while (*p == ' ') p++;
    if (*p == ',') p++;
  }

  if (*p != ']') {
    out.error("expected ']' at end of vector");
    return;
  }

  if (count != dimension) {
    out.error("wrong number of elements");
    return;
  }

  out.set_length(dimension * 4);
}

// Decode: N * 4 bytes binary -> "[f1,f2,...,fN]" string.
// TVECTOR -> STRING
void tvector_to_string(BinaryArg in, StringResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }

  auto src = in.value();
  size_t dimension = src.size() / 4;
  if (dimension == 0 || src.size() % 4 != 0) {
    out.error("argument malformed");
    return;
  }

  auto dst = out.buffer();
  size_t pos = 0;
  if (pos >= dst.size()) {
    out.error("output buffer too small");
    return;
  }
  dst[pos++] = '[';

  for (size_t i = 0; i < dimension; i++) {
    if (i > 0) {
      if (pos >= dst.size()) {
        out.error("output buffer too small");
        return;
      }
      dst[pos++] = ',';
    }
    float val = load_float(src.data() + i * 4);
    int written = snprintf(dst.data() + pos, dst.size() - pos, "%g", val);
    if (written < 0 || pos + static_cast<size_t>(written) >= dst.size()) {
      out.error("output buffer too small");
      return;
    }
    pos += static_cast<size_t>(written);
  }

  if (pos >= dst.size()) {
    out.error("output buffer too small");
    return;
  }
  dst[pos++] = ']';

  out.set_length(pos);
}

// Compare VDF for ORDER BY, indexes: (TVECTOR, TVECTOR) -> INT
// Lexicographic element-by-element comparison.
void tvector_compare(BinaryArg in_l, BinaryArg in_r, IntResult out) {
  auto l = in_l.value();
  auto r = in_r.value();
  size_t dim1 = l.size() / 4;
  size_t dim2 = r.size() / 4;
  size_t min_dim = dim1 < dim2 ? dim1 : dim2;

  for (size_t i = 0; i < min_dim; i++) {
    float v1 = load_float(l.data() + i * 4);
    float v2 = load_float(r.data() + i * 4);
    if (v1 < v2) {
      out.set(-1);
      return;
    }
    if (v1 > v2) {
      out.set(1);
      return;
    }
  }

  if (dim1 < dim2)
    out.set(-1);
  else if (dim1 > dim2)
    out.set(1);
  else
    out.set(0);
}

// Implicit default for TVECTOR(N): writes N zero floats into the buffer.
// buffer_size encodes the resolved persisted_length (N * 4 bytes).
bool tvector_default(int64_t buffer_size, unsigned char *buffer, size_t *length,
                     char * /*error_msg*/) {
  if (buffer_size <= 0 || buffer_size % 4 != 0) {
    *length = 0;
    return true;
  }
  memset(buffer, 0, static_cast<size_t>(buffer_size));
  *length = static_cast<size_t>(buffer_size);
  return false;
}

constexpr const char *TVECTOR = "TVECTOR";

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_tvector", "0.0.1")
        .type(make_type(TVECTOR)
                  .persisted_length(-1)
                  .max_decode_buffer_length(16)
                  .encode("tvector_from_string")
                  .decode("tvector_to_string")
                  .compare("tvector_compare")
                  .int_to_params("tvector_int_to_params")
                  .resolve_params("tvector_resolve_params")
                  .intrinsic_default("tvector_intrinsic_default")
                  .build())
        .func(make_func<&tvector_from_string>("tvector_from_string")
                  .returns(TVECTOR)
                  .param(STRING)
                  .deterministic()
                  .build())
        .func(make_func<&tvector_to_string>("tvector_to_string")
                  .returns(STRING)
                  .param(TVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&tvector_compare>("tvector_compare")
                  .returns(INT)
                  .param(TVECTOR)
                  .param(TVECTOR)
                  .deterministic()
                  .build())
        .func(
            make_int_to_params<&tvector_int_to_params>("tvector_int_to_params"))
        .func(make_resolve_params<&tvector_resolve_params>(
            "tvector_resolve_params"))
        .func(make_intrinsic_default<&tvector_default>(
            "tvector_intrinsic_default")))

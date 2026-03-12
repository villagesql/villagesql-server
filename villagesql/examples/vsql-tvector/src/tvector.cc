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
// TVECTOR supports two element types via the "type" parameter:
//   TVECTOR(N)                          -- N float32 elements (default)
//   TVECTOR('dimension=N,type=float')   -- same as above
//   TVECTOR('dimension=N,type=double')  -- N float64 elements
//
// TVECTOR(3) with float stores 12 bytes (3 * 4). With double, 24 bytes (3 * 8).
// Text format: "[1.0,2.0,3.0]" (comma-separated values in brackets)

#include <villagesql/extension.h>

#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

// Little-endian store/load helpers.

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

void store_double(unsigned char *buf, double val) {
  uint64_t bits;
  memcpy(&bits, &val, sizeof(double));
  buf[0] = static_cast<unsigned char>(bits);
  buf[1] = static_cast<unsigned char>(bits >> 8);
  buf[2] = static_cast<unsigned char>(bits >> 16);
  buf[3] = static_cast<unsigned char>(bits >> 24);
  buf[4] = static_cast<unsigned char>(bits >> 32);
  buf[5] = static_cast<unsigned char>(bits >> 40);
  buf[6] = static_cast<unsigned char>(bits >> 48);
  buf[7] = static_cast<unsigned char>(bits >> 56);
}

double load_double(const unsigned char *buf) {
  uint64_t bits = static_cast<uint64_t>(buf[0]) |
                  (static_cast<uint64_t>(buf[1]) << 8) |
                  (static_cast<uint64_t>(buf[2]) << 16) |
                  (static_cast<uint64_t>(buf[3]) << 24) |
                  (static_cast<uint64_t>(buf[4]) << 32) |
                  (static_cast<uint64_t>(buf[5]) << 40) |
                  (static_cast<uint64_t>(buf[6]) << 48) |
                  (static_cast<uint64_t>(buf[7]) << 56);
  double val;
  memcpy(&val, &bits, sizeof(double));
  return val;
}

// Returns bytes per element based on the "type" parameter.
// Absent or "float" -> 4, "double" -> 8.
size_t bytes_per_element(const std::map<std::string, std::string> &params) {
  auto it = params.find("type");
  if (it != params.end() && it->second == "double") return 8;
  return 4;
}

// Convert TYPE(N) integer to parameter key-value pairs.
// Populates params map with {"dimension": "<N>", "type": "float"}.
// Setting type explicitly ensures TVECTOR(3) produces the same canonical
// params as TVECTOR('dimension=3,type=float').
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
  params["type"] = "float";
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

  // Validate "type" parameter if present.
  auto type_it = params.find("type");
  if (type_it != params.end() && type_it->second != "float" &&
      type_it->second != "double") {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: type must be 'float' or 'double', got '%s'",
             type_it->second.c_str());
    return true;
  }

  size_t bpe = bytes_per_element(params);
  result->persisted_length = dimension * static_cast<int64_t>(bpe);
  // %.17g for double can produce up to ~24 chars; use 32 per element to be safe
  result->max_decode_buffer_length = dimension * (bpe == 8 ? 32 : 16);
  return false;
}

// Encode: "[v1,v2,...,vN]" -> N * bpe bytes binary.
// STRING -> TVECTOR
// Dimension and element type are read from type parameters.
bool tvector_from_string(const std::map<std::string, std::string> &params,
                         std::string_view from,
                         villagesql::Span<unsigned char> buf, size_t *length) {
  auto it = params.find("dimension");
  if (it == params.end()) return true;
  int64_t dimension = strtoll(it->second.c_str(), nullptr, 10);
  if (dimension <= 0) return true;

  size_t bpe = bytes_per_element(params);
  size_t byte_size = static_cast<size_t>(dimension) * bpe;
  if (buf.size() < byte_size) return true;

  // strtof/strtod require a null-terminated string.
  std::string input(from);
  const char *p = input.c_str();

  // Skip leading whitespace
  while (*p == ' ') p++;
  if (*p != '[') return true;
  p++;

  size_t count = 0;
  while (*p != '\0') {
    // Skip whitespace
    while (*p == ' ') p++;
    if (*p == ']') break;

    if (count >= static_cast<size_t>(dimension)) return true;

    char *endptr = nullptr;
    if (bpe == 8) {
      double val = strtod(p, &endptr);
      if (endptr == p) return true;
      store_double(buf.data() + count * bpe, val);
    } else {
      float val = strtof(p, &endptr);
      if (endptr == p) return true;
      store_float(buf.data() + count * bpe, val);
    }
    count++;
    p = endptr;

    // Skip whitespace and comma
    while (*p == ' ') p++;
    if (*p == ',') p++;
  }

  if (*p != ']') return true;
  if (count != static_cast<size_t>(dimension)) return true;

  *length = byte_size;
  return false;
}

// Decode: N * bpe bytes binary -> "[v1,v2,...,vN]" string.
// TVECTOR -> STRING
// Dimension and element type are read from type parameters.
bool tvector_to_string(const std::map<std::string, std::string> &params,
                       villagesql::Span<const unsigned char> data,
                       villagesql::Span<char> out, size_t *out_len) {
  auto it = params.find("dimension");
  if (it == params.end()) return true;
  int64_t dimension = strtoll(it->second.c_str(), nullptr, 10);
  if (dimension <= 0) return true;
  size_t bpe = bytes_per_element(params);
  if (data.size() != static_cast<size_t>(dimension) * bpe) return true;

  size_t pos = 0;
  if (pos >= out.size()) return true;
  out[pos++] = '[';

  for (size_t i = 0; i < static_cast<size_t>(dimension); i++) {
    if (i > 0) {
      if (pos >= out.size()) return true;
      out[pos++] = ',';
    }
    int written;
    if (bpe == 8) {
      double val = load_double(data.data() + i * bpe);
      written = snprintf(out.data() + pos, out.size() - pos, "%.17g", val);
    } else {
      float val = load_float(data.data() + i * bpe);
      written = snprintf(out.data() + pos, out.size() - pos, "%g", val);
    }
    if (written < 0 || pos + static_cast<size_t>(written) >= out.size())
      return true;
    pos += static_cast<size_t>(written);
  }

  if (pos >= out.size()) return true;
  out[pos++] = ']';

  *out_len = pos;
  return false;
}

// Compare: (TVECTOR, TVECTOR) -> INT for ORDER BY, indexes.
// Lexicographic element-by-element comparison.
// Dimension and element type are read from type parameters.
// TODO(villagesql-beta): this state of passing in params and doing a map lookup
// per call is not going to be the end state. We are doing this first for
// correctness. Then we will optimize this so that extension authors don't need
// to do string to int conversions and map lookups. Consider this way of doing
// things relatively short-lived.
// TODO(villagesql-beta): we can also consider having templated versions of
// these functions instead of using branches, then selecting the version to use
// with one branch.
int tvector_compare(const std::map<std::string, std::string> &params,
                    villagesql::Span<const unsigned char> a,
                    villagesql::Span<const unsigned char> b) {
  auto it = params.find("dimension");
  if (it == params.end()) return 0;
  int64_t dimension = strtoll(it->second.c_str(), nullptr, 10);
  if (dimension <= 0) return 0;

  size_t bpe = bytes_per_element(params);
  size_t byte_size = static_cast<size_t>(dimension) * bpe;
  if (a.size() != byte_size || b.size() != byte_size) return 0;

  for (size_t i = 0; i < static_cast<size_t>(dimension); i++) {
    if (bpe == 8) {
      double v1 = load_double(a.data() + i * bpe);
      double v2 = load_double(b.data() + i * bpe);
      if (v1 < v2) return -1;
      if (v1 > v2) return 1;
    } else {
      float v1 = load_float(a.data() + i * bpe);
      float v2 = load_float(b.data() + i * bpe);
      if (v1 < v2) return -1;
      if (v1 > v2) return 1;
    }
  }

  return 0;
}

// Implicit default for TVECTOR: writes N zero elements into the buffer.
// Reads the "dimension" and "type" parameters to determine the byte size.
bool tvector_default(const std::map<std::string, std::string> &params,
                     villagesql::Span<unsigned char> buffer, size_t *length,
                     char *error_msg) {
  // TODO(villagesql-beta): despite looking at dimension here and validating it,
  // we could skip this part if we rely on the length of the buffer to be the
  // memory we zero out.
  auto it = params.find("dimension");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR intrinsic_default: missing dimension");
    return true;
  }
  int64_t dimension = strtoll(it->second.c_str(), nullptr, 10);
  if (dimension <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR intrinsic_default: invalid dimension");
    return true;
  }
  size_t bpe = bytes_per_element(params);
  size_t byte_size = static_cast<size_t>(dimension) * bpe;
  if (byte_size > buffer.size()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR intrinsic_default: buffer too small");
    return true;
  }
  memset(buffer.data(), 0, byte_size);
  *length = byte_size;
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
        .func(make_type_encode<&tvector_from_string>("tvector_from_string",
                                                     TVECTOR))
        .func(make_type_decode<&tvector_to_string>("tvector_to_string",
                                                   TVECTOR))
        .func(make_type_compare<&tvector_compare>("tvector_compare", TVECTOR))
        .func(
            make_int_to_params<&tvector_int_to_params>("tvector_int_to_params"))
        .func(make_resolve_params<&tvector_resolve_params>(
            "tvector_resolve_params"))
        .func(make_intrinsic_default<&tvector_default>(
            "tvector_intrinsic_default", TVECTOR)))

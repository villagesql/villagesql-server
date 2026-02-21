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
#include <string>

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
// Produces {key:"dimension", value:"<N>"}.
bool tvector_int_to_params(int64_t value, vef_type_param_t *params,
                           size_t *param_count, char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR dimension must be a positive integer, got %" PRId64,
             value);
    return true;
  }
  static char dim_buf[32];
  snprintf(dim_buf, sizeof(dim_buf), "%" PRId64, value);
  params[0] = {"dimension", dim_buf};
  *param_count = 1;
  return false;
}

// Validate type parameters and compute storage characteristics.
bool tvector_resolve_params(const vef_type_param_t *params, size_t param_count,
                            vef_type_resolved_params_t *result,
                            char *error_msg) {
  int64_t dimension = 0;
  for (size_t i = 0; i < param_count; i++) {
    if (strcmp(params[i].key, "dimension") == 0) {
      char *endptr = nullptr;
      dimension = strtoll(params[i].value, &endptr, 10);
      if (endptr == params[i].value || *endptr != '\0') {
        snprintf(error_msg, VEF_MAX_ERROR_LEN,
                 "TVECTOR: invalid dimension value '%s'", params[i].value);
        return true;
      }
      break;
    }
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
// Dimension is inferred from buffer_size / 4.
bool tvector_encode(unsigned char *buffer, size_t buffer_size, const char *from,
                    size_t from_len, size_t *length) {
  if (buffer == nullptr || buffer_size == 0) {
    *length = 0;
    return true;
  }

  size_t dimension = buffer_size / 4;
  if (dimension == 0) {
    *length = 0;
    return true;
  }

  std::string input(from, from_len);
  const char *p = input.c_str();

  // Skip leading whitespace
  while (*p == ' ') p++;
  if (*p != '[') {
    *length = SIZE_MAX;
    return true;
  }
  p++;

  size_t count = 0;
  while (*p != '\0') {
    // Skip whitespace
    while (*p == ' ') p++;
    if (*p == ']') break;

    if (count >= dimension) {
      snprintf(reinterpret_cast<char *>(buffer), buffer_size,
               "too many elements");
      *length = SIZE_MAX;
      return true;
    }

    char *endptr = nullptr;
    float val = strtof(p, &endptr);
    if (endptr == p) {
      *length = SIZE_MAX;
      return true;
    }
    store_float(buffer + count * 4, val);
    count++;
    p = endptr;

    // Skip whitespace and comma
    while (*p == ' ') p++;
    if (*p == ',') p++;
  }

  if (*p != ']') {
    *length = SIZE_MAX;
    return true;
  }

  if (count != dimension) {
    *length = SIZE_MAX;
    return true;
  }

  *length = dimension * 4;
  return false;
}

// Decode: N * 4 bytes binary -> "[f1,f2,...,fN]" string.
bool tvector_decode(const unsigned char *buffer, size_t buffer_size, char *to,
                    size_t to_buffer_size, size_t *to_length) {
  if (buffer == nullptr || to == nullptr || to_length == nullptr) {
    return true;
  }

  size_t dimension = buffer_size / 4;
  if (dimension == 0 || buffer_size % 4 != 0) {
    return true;
  }

  size_t pos = 0;
  if (pos >= to_buffer_size) return true;
  to[pos++] = '[';

  for (size_t i = 0; i < dimension; i++) {
    if (i > 0) {
      if (pos >= to_buffer_size) return true;
      to[pos++] = ',';
    }
    float val = load_float(buffer + i * 4);
    int written = snprintf(to + pos, to_buffer_size - pos, "%g", val);
    if (written < 0 || pos + static_cast<size_t>(written) >= to_buffer_size) {
      return true;
    }
    pos += static_cast<size_t>(written);
  }

  if (pos >= to_buffer_size) return true;
  to[pos++] = ']';

  *to_length = pos;
  return false;
}

// Compare: lexicographic element-by-element comparison.
int tvector_compare(const unsigned char *data1, size_t len1,
                    const unsigned char *data2, size_t len2) {
  size_t dim1 = len1 / 4;
  size_t dim2 = len2 / 4;
  size_t min_dim = dim1 < dim2 ? dim1 : dim2;

  for (size_t i = 0; i < min_dim; i++) {
    float v1 = load_float(data1 + i * 4);
    float v2 = load_float(data2 + i * 4);
    if (v1 < v2) return -1;
    if (v1 > v2) return 1;
  }

  if (dim1 < dim2) return -1;
  if (dim1 > dim2) return 1;
  return 0;
}

VEF_GENERATE_ENTRY_POINTS(make_extension("vsql_tvector", "0.0.1")
                              .type(make_type("TVECTOR")
                                        .persisted_length(-1)
                                        .max_decode_buffer_length(16)
                                        .encode(&tvector_encode)
                                        .decode(&tvector_decode)
                                        .compare(&tvector_compare)
                                        .int_to_params(&tvector_int_to_params)
                                        .resolve_params(&tvector_resolve_params)
                                        .build()))
